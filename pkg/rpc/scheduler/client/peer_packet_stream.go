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

type peerPacketStream struct {
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

func newPeerPacketStream(sc *schedulerClient, ctx context.Context, taskId string, ptr *scheduler.PeerTaskRequest, opts []grpc.CallOption, prc chan *scheduler.PieceResult) (*peerPacketStream, error) {
	pps := &peerPacketStream{
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
	pps.client, pps.nextNum = xc.(scheduler.SchedulerClient), nextNum

	if err := pps.initStream(); err != nil {
		return nil, err
	} else {
		return pps, nil
	}
}

func (pps *peerPacketStream) send(pr *scheduler.PieceResult) error {
	if pr.Success {
		pps.lastPieceResult = pr
	}

	err := pps.stream.Send(pr)
	if err !=nil && pr.PieceNum != base.END_OF_PIECE{
		err = pps.retrySend(pr)
	}
	return err
}

func (pps *peerPacketStream) closeSend() error {
	return pps.stream.CloseSend()
}

func (pps *peerPacketStream) recv() (pp *scheduler.PeerPacket, err error) {
	if pp, err = pps.stream.Recv(); err != nil {
		pp, err = pps.retryRecv(err)
	}

	// re-register
	if err == nil && pp.State.Code == base.Code_PEER_TASK_NOT_REGISTERED {
		_, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
			timeCtx, _ := context.WithTimeout(context.Background(), time.Duration(10)*time.Second)
			return pps.client.RegisterPeerTask(timeCtx, pps.ptr)
		}, pps.InitBackoff, pps.MaxBackOff, pps.MaxAttempts)

		if err == nil {
			pp = &scheduler.PeerPacket{
				State:  base.NewState(base.Code_SUCCESS, nil),
				TaskId: pps.taskId,
				SrcPid: pps.ptr.PeerId,
			}

			pps.prc <- pps.lastPieceResult
		}
	}
	return
}

func (pps *peerPacketStream) initStream() error {
	stream, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		return pps.client.ReportPieceResult(pps.ctx, pps.opts...)
	}, pps.InitBackoff, pps.MaxBackOff, pps.MaxAttempts)

	if err != nil {
		return pps.replaceClient(err)
	}

	pps.stream = stream.(scheduler.Scheduler_ReportPieceResultClient)
	pps.Times = 1

	return err
}

func (pps *peerPacketStream) retryRecv(cause error) (*scheduler.PeerPacket, error) {
	code := status.Code(cause)
	if code == codes.DeadlineExceeded || code == codes.Aborted {
		return nil, cause
	}

	var needMig = code == codes.FailedPrecondition
	if !needMig {
		if cause = pps.replaceStream(); cause != nil {
			needMig = true
		}
	}

	if needMig {
		if err := pps.replaceClient(cause); err != nil {
			return nil, err
		}
	}

	pps.prc <- pps.lastPieceResult

	return pps.recv()
}

func (pps *peerPacketStream) replaceStream() error {
	if pps.Times >= pps.MaxAttempts {
		return errors.New("times of replacing stream reaches limit")
	}

	res, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		return pps.client.ReportPieceResult(pps.ctx, pps.opts...)
	}, pps.InitBackoff, pps.MaxBackOff, pps.MaxAttempts)

	if err == nil {
		pps.stream = res.(scheduler.Scheduler_ReportPieceResultClient)
		pps.Times++
	}

	return err
}

func (pps *peerPacketStream) replaceClient(cause error) error {
	if err := pps.sc.TryMigrate(pps.nextNum, cause); err != nil {
		return err
	}

	xc, _, nextNum := pps.sc.GetClientSafely()
	pps.client, pps.nextNum = xc.(scheduler.SchedulerClient), nextNum

	stream, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		timeCtx, _ := context.WithTimeout(context.Background(), time.Duration(10)*time.Second)
		rr, err := pps.client.RegisterPeerTask(timeCtx, pps.ptr)

		if err == nil && rr.State.Success {
			return pps.client.ReportPieceResult(pps.ctx, pps.opts...)
		} else {
			if err == nil {
				err = errors.New(rr.State.Msg)
			}
			return nil, err
		}
	}, pps.InitBackoff, pps.MaxBackOff, pps.MaxAttempts)

	if err != nil {
		return pps.replaceClient(err)
	} else {
		pps.stream = stream.(scheduler.Scheduler_ReportPieceResultClient)
		pps.Times = 1
	}

	return nil
}
