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

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/base/common"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
)

//go:generate mockgen -package mocks -source peer_packet_stream.go -destination ./mocks/peer_packet_stream_mock.go
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
			MaxAttempts: 3,
			InitBackoff: 0.2,
			MaxBackOff:  2.0,
		},
	}

	if err := pps.initStream(); err != nil {
		return nil, err
	}
	return pps, nil
}

func (pps *peerPacketStream) Send(pr *scheduler.PieceResult) error {
	pps.lastPieceResult = pr
	pps.sc.UpdateAccessNodeMapByHashKey(pps.hashKey)

	if err := pps.stream.Send(pr); err != nil {
		if err := pps.closeSend(); err != nil {
			return err
		}
		return err
	}

	if pr.PieceInfo.PieceNum == common.EndOfPiece {
		if err := pps.closeSend(); err != nil {
			return err
		}
	}

	return nil
}

func (pps *peerPacketStream) closeSend() error {
	return pps.stream.CloseSend()
}

func (pps *peerPacketStream) Recv() (pp *scheduler.PeerPacket, err error) {
	pps.sc.UpdateAccessNodeMapByHashKey(pps.hashKey)
	return pps.stream.Recv()
}

func (pps *peerPacketStream) retrySend(pr *scheduler.PieceResult, cause error) error {
	if status.Code(cause) == codes.DeadlineExceeded || status.Code(cause) == codes.Canceled {
		return cause
	}

	if err := pps.replaceStream(cause); err != nil {
		return cause
	}

	return pps.Send(pr)
}

func (pps *peerPacketStream) retryRecv(cause error) (*scheduler.PeerPacket, error) {
	if status.Code(cause) == codes.DeadlineExceeded || status.Code(cause) == codes.Canceled {
		return nil, cause
	}
	_, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		client, _, err := pps.sc.getSchedulerClient(pps.hashKey, false)
		if err != nil {
			return nil, err
		}
		//timeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		//defer cancel()
		_, err = client.RegisterPeerTask(pps.ctx, pps.ptr)
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
	var target string
	stream, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		var client scheduler.SchedulerClient
		var err error
		client, target, err = pps.sc.getSchedulerClient(pps.hashKey, true)
		if err != nil {
			return nil, err
		}
		return client.ReportPieceResult(pps.ctx, pps.opts...)
	}, pps.retryMeta.InitBackoff, pps.retryMeta.MaxBackOff, pps.retryMeta.MaxAttempts, nil)
	if err != nil {
		if errors.Cause(err) == dferrors.ErrNoCandidateNode {
			return errors.Wrapf(err, "get grpc server instance failed")
		}
		logger.WithTaskID(pps.hashKey).Infof("initStream: invoke scheduler node %s ReportPieceResult failed: %v", target, err)
		return pps.replaceClient(err)
	}
	pps.stream = stream.(scheduler.Scheduler_ReportPieceResultClient)
	pps.retryMeta.StreamTimes = 1
	return nil
}

func (pps *peerPacketStream) replaceStream(cause error) error {
	if pps.retryMeta.StreamTimes >= pps.retryMeta.MaxAttempts {
		return cause
	}
	var target string
	res, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		var client scheduler.SchedulerClient
		var err error
		client, target, err = pps.sc.getSchedulerClient(pps.hashKey, true)
		if err != nil {
			return nil, err
		}
		return client.ReportPieceResult(pps.ctx, pps.opts...)
	}, pps.retryMeta.InitBackoff, pps.retryMeta.MaxBackOff, pps.retryMeta.MaxAttempts, cause)
	if err != nil {
		logger.WithTaskID(pps.hashKey).Infof("replaceStream: invoke scheduler node %s ReportPieceResult failed: %v", target, err)
		return pps.replaceStream(cause)
	}
	pps.stream = res.(scheduler.Scheduler_ReportPieceResultClient)
	pps.retryMeta.StreamTimes++
	return nil
}

func (pps *peerPacketStream) replaceClient(cause error) error {
	preNode, err := pps.sc.TryMigrate(pps.hashKey, cause, pps.failedServers)
	if err != nil {
		logger.WithTaskID(pps.hashKey).Infof("replaceClient: tryMigrate scheduler node failed: %v", err)
		return cause
	}
	pps.failedServers = append(pps.failedServers, preNode)
	var target string
	stream, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		var client scheduler.SchedulerClient
		var err error
		client, target, err = pps.sc.getSchedulerClient(pps.hashKey, true)
		if err != nil {
			return nil, err
		}
		_, err = client.RegisterPeerTask(pps.ctx, pps.ptr)
		if err != nil {
			return nil, err
		}
		return client.ReportPieceResult(pps.ctx, pps.opts...)
	}, pps.retryMeta.InitBackoff, pps.retryMeta.MaxBackOff, pps.retryMeta.MaxAttempts, cause)
	if err != nil {
		logger.WithTaskID(pps.hashKey).Infof("replaceClient: invoke scheduler node %s ReportPieceResult failed: %v", target, err)
		return pps.replaceClient(cause)
	}
	pps.stream = stream.(scheduler.Scheduler_ReportPieceResultClient)
	pps.retryMeta.StreamTimes = 1
	return nil
}
