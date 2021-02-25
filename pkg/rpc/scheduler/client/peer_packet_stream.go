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
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/dferrors"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/base/common"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"time"
)

type peerPacketStream struct {
	sc      *schedulerClient
	ctx     context.Context
	hashKey string
	ptr     *scheduler.PeerTaskRequest
	opts    []grpc.CallOption
	prc     chan *scheduler.PieceResult

	// stream for one client
	stream scheduler.Scheduler_ReportPieceResultClient
	failedServers []string
	lastPieceResult *scheduler.PieceResult

	rpc.RetryMeta
}

func newPeerPacketStream(sc *schedulerClient, ctx context.Context, hashKey string, ptr *scheduler.PeerTaskRequest, opts []grpc.CallOption, prc chan *scheduler.PieceResult) (*peerPacketStream, error) {
	ptr.IsMigrating = true

	pps := &peerPacketStream{
		sc:      sc,
		ctx:     ctx,
		hashKey: hashKey,
		ptr:     ptr,
		opts:    opts,
		prc:     prc,
		RetryMeta: rpc.RetryMeta{
			MaxAttempts: 5,
			InitBackoff: 0.5,
			MaxBackOff:  4.0,
		},
	}

	if err := pps.initStream(); err != nil {
		return nil, err
	} else {
		return pps, nil
	}
}

func (pps *peerPacketStream) send(pr *scheduler.PieceResult) (err error) {
	pps.lastPieceResult = pr
	pps.sc.UpdateAccessNodeMap(pps.hashKey)
	err = pps.stream.Send(pr)

	if pr.PieceNum == common.EndOfPiece {
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
	pps.sc.UpdateAccessNodeMap(pps.hashKey)
	pp, err = pps.stream.Recv()

	if err != nil {
		if e, ok := err.(*dferrors.DfError); ok && e.Code == dfcodes.PeerTaskNotRegistered {
			_, err = rpc.ExecuteWithRetry(func() (interface{}, error) {
				timeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				rr, err := pps.sc.getSchedulerClient(pps.hashKey).RegisterPeerTask(timeCtx, pps.ptr)
				if err == nil && rr.State.Success {
					pps.prc <- pps.lastPieceResult
				} else {
					if err == nil {
						err = errors.New(rr.State.Msg)
					}
				}
				return rr, err
			}, pps.InitBackoff, pps.MaxBackOff, pps.MaxAttempts, nil)
		}
	}

	return
}

func (pps *peerPacketStream) retrySend(pr *scheduler.PieceResult, cause error) error {
	if status.Code(cause) == codes.DeadlineExceeded {
		return cause
	}

	if err := pps.replaceStream(cause); err != nil {
		if err := pps.replaceClient(cause); err != nil {
			return cause
		}
	}

	return pps.send(pr)
}

func (pps *peerPacketStream) initStream() error {
	stream, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		return pps.sc.getSchedulerClient(pps.hashKey).ReportPieceResult(pps.ctx, pps.opts...)
	}, pps.InitBackoff, pps.MaxBackOff, pps.MaxAttempts, nil)

	if err != nil {
		err = pps.replaceClient(err)
	} else {
		pps.stream = stream.(scheduler.Scheduler_ReportPieceResultClient)
		pps.StreamTimes = 1
	}

	return err
}

func (pps *peerPacketStream) replaceStream(cause error) error {
	if pps.StreamTimes >= pps.MaxAttempts {
		return errors.New("times of replacing stream reaches limit")
	}

	res, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		return pps.sc.getSchedulerClient(pps.hashKey).ReportPieceResult(pps.ctx, pps.opts...)
	}, pps.InitBackoff, pps.MaxBackOff, pps.MaxAttempts, cause)

	if err == nil {
		pps.stream = res.(scheduler.Scheduler_ReportPieceResultClient)
		pps.StreamTimes++
	}

	return err
}

func (pps *peerPacketStream) replaceClient(cause error) error {
	if preNode, err := pps.sc.TryMigrate(pps.hashKey, cause, pps.failedServers); err != nil {
		return err
	} else {
		pps.failedServers = append(pps.failedServers, preNode)
	}

	stream, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		timeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		rr, err := pps.sc.getSchedulerClient(pps.hashKey).RegisterPeerTask(timeCtx, pps.ptr)

		if err == nil && rr.State.Success {
			return pps.sc.getSchedulerClient(pps.hashKey).ReportPieceResult(pps.ctx, pps.opts...)
		} else {
			if err == nil {
				err = errors.New(rr.State.Msg)
			}
			return nil, err
		}
	}, pps.InitBackoff, pps.MaxBackOff, pps.MaxAttempts, cause)

	if err != nil {
		err = pps.replaceClient(cause)
	} else {
		pps.stream = stream.(scheduler.Scheduler_ReportPieceResultClient)
		pps.StreamTimes = 1
	}

	return err
}