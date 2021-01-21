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
	ctx    context.Context
	taskId string
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
	ptr.IsMigrating = true

	pps := &peerPacketStream{
		sc:              sc,
		ctx:             ctx,
		taskId:          taskId,
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

func (pps *peerPacketStream) send(pr *scheduler.PieceResult) (err error) {
	pps.lastPieceResult = pr

	err = pps.stream.Send(pr)

	if pr.PieceNum == base.END_OF_PIECE {
		return
	}

	if err != nil {
		err = pps.retrySend(pr, err)
	}

	return
}

func (pps *peerPacketStream) closeSend() error {
	return pps.stream.CloseSend()
}

func (pps *peerPacketStream) recv() (pp *scheduler.PeerPacket, err error) {
	pp, err = pps.stream.Recv()

	if err == nil && pp.State.Code == base.Code_PEER_TASK_NOT_REGISTERED {
		_, err = rpc.ExecuteWithRetry(func() (interface{}, error) {
			timeCtx, _ := context.WithTimeout(context.Background(), 5*time.Second)
			rr, err := pps.client.RegisterPeerTask(timeCtx, pps.ptr)
			if err == nil && rr.State.Success {
				pps.prc <- pps.lastPieceResult
			} else {
				if err == nil {
					err = errors.New(rr.State.Msg)
				}
			}
			return rr, err
		}, pps.InitBackoff, pps.MaxBackOff, pps.MaxAttempts)
	}

	return
}

func (pps *peerPacketStream) retrySend(pr *scheduler.PieceResult, cause error) error {
	if status.Code(cause) == codes.DeadlineExceeded {
		return cause
	}

	if err := pps.replaceStream(); err != nil {
		if err = pps.replaceClient(cause); err != nil {
			return err
		}
	}

	return pps.send(pr)
}

func (pps *peerPacketStream) initStream() error {
	stream, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		return pps.client.ReportPieceResult(pps.ctx, pps.opts...)
	}, pps.InitBackoff, pps.MaxBackOff, pps.MaxAttempts)

	if err != nil {
		err = pps.replaceClient(err)
	} else {
		pps.stream = stream.(scheduler.Scheduler_ReportPieceResultClient)
		pps.StreamTimes = 1
	}

	return err
}

func (pps *peerPacketStream) replaceStream() error {
	if pps.StreamTimes >= pps.MaxAttempts {
		return errors.New("times of replacing stream reaches limit")
	}

	res, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		return pps.client.ReportPieceResult(pps.ctx, pps.opts...)
	}, pps.InitBackoff, pps.MaxBackOff, pps.MaxAttempts)

	if err == nil {
		pps.stream = res.(scheduler.Scheduler_ReportPieceResultClient)
		pps.StreamTimes++
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
		timeCtx, _ := context.WithTimeout(context.Background(), 5*time.Second)
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
		err = pps.replaceClient(err)
	} else {
		pps.stream = stream.(scheduler.Scheduler_ReportPieceResultClient)
		pps.StreamTimes = 1
	}

	return err
}
