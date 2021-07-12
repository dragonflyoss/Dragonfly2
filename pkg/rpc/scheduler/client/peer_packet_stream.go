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
	"io"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"d7y.io/dragonfly.v2/pkg/rpc"
	"d7y.io/dragonfly.v2/pkg/rpc/base/common"
	"d7y.io/dragonfly.v2/pkg/rpc/scheduler"
)

type PeerPacketStream interface {
	Recv() (pp *scheduler.PeerPacket, err error)
	Send(pr *scheduler.PieceResult) (err error)
}

type peerPacketStream struct {
	sc      *schedulerClient
	ctx     context.Context
	hashKey string
	ptr     *scheduler.PeerTaskRequest
	opts    []grpc.CallOption

	// stream for one client
	stream          scheduler.Scheduler_ReportPieceResultClient
	failedServers   []string
	lastPieceResult *scheduler.PieceResult

	retryMeta rpc.RetryMeta
}

func newPeerPacketStream(ctx context.Context, sc *schedulerClient, hashKey string, ptr *scheduler.PeerTaskRequest, opts []grpc.CallOption) (PeerPacketStream, error) {
	ptr.IsMigrating = true

	pps := &peerPacketStream{
		sc:      sc,
		ctx:     ctx,
		hashKey: hashKey,
		ptr:     ptr,
		opts:    opts,
		retryMeta: rpc.RetryMeta{
			MaxAttempts: 5,
			InitBackoff: 0.5,
			MaxBackOff:  4.0,
		},
	}

	if err := pps.initStream(); err != nil {
		return nil, err
	}
	return pps, nil
}

func (pps *peerPacketStream) Send(pr *scheduler.PieceResult) (err error) {
	pps.lastPieceResult = pr
	pps.sc.UpdateAccessNodeMapByHashKey(pps.hashKey)
	err = pps.stream.Send(pr)

	if pr.PieceNum == common.EndOfPiece {
		pps.closeSend()
		return
	}

	if err != nil {
		pps.closeSend()
		err = pps.retrySend(pr, err)
	}

	return
}

func (pps *peerPacketStream) closeSend() error {
	return pps.stream.CloseSend()
}

func (pps *peerPacketStream) Recv() (pp *scheduler.PeerPacket, err error) {
	pps.sc.UpdateAccessNodeMapByHashKey(pps.hashKey)
	if pp, err = pps.stream.Recv(); err != nil && err != io.EOF {
		pp, err = pps.retryRecv(err)
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

	return pps.Send(pr)
}

func (pps *peerPacketStream) retryRecv(cause error) (*scheduler.PeerPacket, error) {
	if status.Code(cause) == codes.DeadlineExceeded {
		return nil, cause
	}
	_, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		client, _, err := pps.sc.getSchedulerClient(pps.hashKey, false)
		if err != nil {
			return nil, err
		}
		timeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err = client.RegisterPeerTask(timeCtx, pps.ptr)
		if err != nil {
			return nil, err
		}
		stream, err := client.ReportPieceResult(pps.ctx, pps.opts...)
		if err != nil {
			return nil, err
		}
		pps.stream = stream.(scheduler.Scheduler_ReportPieceResultClient)
		pps.retryMeta.StreamTimes = 1
		err = pps.Send(pps.lastPieceResult)
		if err != nil {
			return nil, err
		}
		return nil, nil
	}, pps.retryMeta.InitBackoff, pps.retryMeta.MaxBackOff, pps.retryMeta.MaxAttempts, nil)
	if err != nil {
		return nil, cause
	}
	return pps.Recv()
}

func (pps *peerPacketStream) initStream() error {
	stream, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		client, _, err := pps.sc.getSchedulerClient(pps.hashKey, true)
		if err != nil {
			return nil, err
		}
		return client.ReportPieceResult(pps.ctx, pps.opts...)
	}, pps.retryMeta.InitBackoff, pps.retryMeta.MaxBackOff, pps.retryMeta.MaxAttempts, nil)
	if err == nil {
		pps.stream = stream.(scheduler.Scheduler_ReportPieceResultClient)
		pps.retryMeta.StreamTimes = 1
	}
	if err != nil {
		err = pps.replaceClient(err)
	}
	return err
}

func (pps *peerPacketStream) replaceStream(cause error) error {
	if pps.retryMeta.StreamTimes >= pps.retryMeta.MaxAttempts {
		return errors.New("times of replacing stream reaches limit")
	}
	res, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		client, _, err := pps.sc.getSchedulerClient(pps.hashKey, true)
		if err != nil {
			return nil, err
		}
		return client.ReportPieceResult(pps.ctx, pps.opts...)
	}, pps.retryMeta.InitBackoff, pps.retryMeta.MaxBackOff, pps.retryMeta.MaxAttempts, cause)
	if err == nil {
		pps.stream = res.(scheduler.Scheduler_ReportPieceResultClient)
		pps.retryMeta.StreamTimes++
	}
	return err
}

func (pps *peerPacketStream) replaceClient(cause error) error {
	preNode, err := pps.sc.TryMigrate(pps.hashKey, cause, pps.failedServers)
	if err != nil {
		return err
	}
	pps.failedServers = append(pps.failedServers, preNode)

	stream, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		client, _, err := pps.sc.getSchedulerClient(pps.hashKey, true)
		if err != nil {
			return nil, err
		}
		timeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err = client.RegisterPeerTask(timeCtx, pps.ptr)
		if err != nil {
			return nil, err
		}
		return client.ReportPieceResult(pps.ctx, pps.opts...)
	}, pps.retryMeta.InitBackoff, pps.retryMeta.MaxBackOff, pps.retryMeta.MaxAttempts, cause)
	if err == nil {
		pps.stream = stream.(scheduler.Scheduler_ReportPieceResultClient)
		pps.retryMeta.StreamTimes = 1
	}
	if err != nil {
		err = pps.replaceClient(cause)
	}
	return err
}
