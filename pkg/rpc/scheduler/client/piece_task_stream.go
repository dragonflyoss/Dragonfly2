/*
 *     Copyright 2020 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package client

import (
	"context"
	"errors"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"time"
)

type pieceTaskStream struct {
	sc     *schedulerClient
	taskId string
	ctx    context.Context
	ptr    *scheduler.PeerTaskRequest
	opts   []grpc.CallOption
	prc    chan *scheduler.PieceResult

	// client for one target
	client  scheduler.SchedulerClient
	nextNum int
	// stream for one client
	stream scheduler.Scheduler_ReportPieceResultClient

	lastPieceResult *scheduler.PieceResult

	rpc.RetryMeta
}

func newPeerPacketStream(sc *schedulerClient, ctx context.Context, taskId string, ptr *scheduler.PeerTaskRequest, opts []grpc.CallOption, prc chan *scheduler.PieceResult) (*pieceTaskStream, error) {
	pts := &pieceTaskStream{
		sc:              sc,
		taskId:          taskId,
		ctx:             ctx,
		ptr:             ptr,
		opts:            opts,
		prc:             prc,
		lastPieceResult: scheduler.NewZeroPieceResult(taskId, ptr.PeerId),
		RetryMeta: rpc.RetryMeta{
			MaxAttempts: 5,
			InitBackoff: 0.5,
			MaxBackOff:  4.0,
		},
	}

	xc, _, nextNum := sc.GetClientSafely()
	pts.client, pts.nextNum = xc.(scheduler.SchedulerClient), nextNum

	if err := pts.initStream(); err != nil {
		return nil, err
	} else {
		return pts, nil
	}
}

func (pts *pieceTaskStream) send(pr *scheduler.PieceResult) error {
	if pr.Success {
		pts.lastPieceResult = pr
	}

	return pts.stream.Send(pr)
}

func (pts *pieceTaskStream) closeSend() error {
	return pts.stream.CloseSend()
}

func (pts *pieceTaskStream) recv() (pp *scheduler.PeerPacket, err error) {
	if pp, err = pts.stream.Recv(); err != nil {
		pp, err = pts.retryRecv(err)
	}

	// re-register
	if err == nil && pp.State.Code == base.Code_PEER_TASK_NOT_REGISTERED {
		_, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
			timeCtx, _ := context.WithTimeout(context.Background(), time.Duration(10)*time.Second)
			return pts.client.RegisterPeerTask(timeCtx, pts.ptr)
		}, pts.InitBackoff, pts.MaxBackOff, pts.MaxAttempts)

		if err == nil {
			pp = &scheduler.PeerPacket{
				State:  base.NewState(base.Code_SUCCESS, nil),
				TaskId: pts.taskId,
				SrcPid: pts.ptr.PeerId,
			}

			pts.prc <- pts.lastPieceResult
		}
	}
	return
}

func (pts *pieceTaskStream) initStream() error {
	stream, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		return pts.client.ReportPieceResult(pts.ctx, pts.opts...)
	}, pts.InitBackoff, pts.MaxBackOff, pts.MaxAttempts)

	if err != nil {
		return pts.replaceClient(err)
	}

	pts.stream = stream.(scheduler.Scheduler_ReportPieceResultClient)
	pts.Times = 1

	return err
}

func (pts *pieceTaskStream) retryRecv(cause error) (*scheduler.PeerPacket, error) {
	code := status.Code(cause)
	if code == codes.DeadlineExceeded || code == codes.Aborted {
		return nil, cause
	}

	var needMig = code == codes.FailedPrecondition
	if !needMig {
		if cause = pts.replaceStream(); cause != nil {
			needMig = true
		}
	}

	if needMig {
		if err := pts.replaceClient(cause); err != nil {
			return nil, err
		}
	}

	pts.prc <- pts.lastPieceResult

	return pts.recv()
}

func (pts *pieceTaskStream) replaceStream() error {
	if pts.Times >= pts.MaxAttempts {
		return errors.New("times of replacing stream reaches limit")
	}

	res, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		return pts.client.ReportPieceResult(pts.ctx, pts.opts...)
	}, pts.InitBackoff, pts.MaxBackOff, pts.MaxAttempts)

	if err == nil {
		pts.stream = res.(scheduler.Scheduler_ReportPieceResultClient)
		pts.Times++
	}

	return err
}

func (pts *pieceTaskStream) replaceClient(cause error) error {
	if err := pts.sc.TryMigrate(pts.nextNum, cause); err != nil {
		return err
	}

	xc, _, nextNum := pts.sc.GetClientSafely()
	pts.client, pts.nextNum = xc.(scheduler.SchedulerClient), nextNum

	stream, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		timeCtx, _ := context.WithTimeout(context.Background(), time.Duration(10)*time.Second)
		rr, err := pts.client.RegisterPeerTask(timeCtx, pts.ptr)

		if err == nil && rr.State.Success {
			return pts.client.ReportPieceResult(pts.ctx, pts.opts...)
		} else {
			if err == nil {
				err = errors.New(rr.State.Msg)
			}
			return nil, err
		}
	}, pts.InitBackoff, pts.MaxBackOff, pts.MaxAttempts)

	if err != nil {
		return pts.replaceClient(err)
	} else {
		pts.stream = stream.(scheduler.Scheduler_ReportPieceResultClient)
		pts.Times = 1
	}

	return nil
}
