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
	"fmt"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/dfnet"
	"d7y.io/dragonfly/v2/internal/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
)

func GetClientByAddr(addrs []dfnet.NetAddr, opts ...grpc.DialOption) (SchedulerClient, error) {
	if len(addrs) == 0 {
		return nil, errors.New("address list of scheduler is empty")
	}
	sc := &schedulerClient{
		rpc.NewConnection(context.Background(), rpc.SchedulerScheme, addrs, []rpc.ConnOption{
			rpc.WithDialOption(opts),
		}),
	}
	logger.Infof("scheduler server list: %s", addrs)
	return sc, nil
}

// SchedulerClient see scheduler.SchedulerClient
type SchedulerClient interface {
	// RegisterPeerTask register peer task to scheduler
	RegisterPeerTask(context.Context, *scheduler.PeerTaskRequest, ...grpc.CallOption) (*scheduler.RegisterResult, error)
	// ReportPieceResult IsMigrating of ptr will be set to true
	ReportPieceResult(context.Context, string, *scheduler.PeerTaskRequest, ...grpc.CallOption) (PeerPacketStream, error)

	ReportPeerResult(context.Context, *scheduler.PeerResult, ...grpc.CallOption) error

	LeaveTask(context.Context, *scheduler.PeerTarget, ...grpc.CallOption) error

	UpdateState(addrs []dfnet.NetAddr)

	Close() error
}

type schedulerClient struct {
	*rpc.Connection
}

func (sc *schedulerClient) getSchedulerClient() (scheduler.SchedulerClient, error) {
	// "scheduler.Scheduler" is the scheduler._Scheduler_serviceDesc.ServiceName
	clientConn, err := sc.Connection.GetConsistentHashClient(fmt.Sprintf("%s:///%s", rpc.SchedulerScheme, "scheduler.Scheduler"))
	if err != nil {
		return nil, err
	}
	return scheduler.NewSchedulerClient(clientConn), nil
}

func (sc *schedulerClient) RegisterPeerTask(ctx context.Context, ptr *scheduler.PeerTaskRequest, opts ...grpc.CallOption) (*scheduler.RegisterResult, error) {
	var (
		taskID string
		res    interface{}
	)
	key := idgen.TaskID(ptr.Url, ptr.UrlMeta)
	logger.WithTaskAndPeerID(key, ptr.PeerId).Infof("generate hash key taskId: %s and start to register peer task for peer_id(%s) url(%s)", key, ptr.PeerId,
		ptr.Url)
	reg := func() (interface{}, error) {
		var client scheduler.SchedulerClient
		var err error
		client, err = sc.getSchedulerClient()
		if err != nil {
			return nil, err
		}
		return client.RegisterPeerTask(context.WithValue(ctx, rpc.PickKey{}, &rpc.PickReq{Key: key, Attempt: 1}), ptr, opts...)
	}
	res, err := rpc.ExecuteWithRetry(reg, 0.2, 2.0, 3, nil)
	if err != nil {
		logger.WithTaskAndPeerID(key, ptr.PeerId).Errorf("RegisterPeerTask: register peer task to scheduler failed: %v", err)
		return sc.retryRegisterPeerTask(ctx, key, ptr, err, opts)
	}
	rr := res.(*scheduler.RegisterResult)
	taskID = rr.TaskId
	if taskID != key {
		logger.WithTaskAndPeerID(taskID, ptr.PeerId).Warnf("register peer task correct taskId from %s to %s", key, taskID)
	}
	logger.WithTaskAndPeerID(taskID, ptr.PeerId).
		Infof("register peer task result success url: %s", ptr.Url)
	return rr, err
}

func (sc *schedulerClient) retryRegisterPeerTask(ctx context.Context, hashKey string, ptr *scheduler.PeerTaskRequest, cause error,
	opts []grpc.CallOption) (*scheduler.RegisterResult, error) {
	if status.Code(cause) == codes.Canceled || status.Code(cause) == codes.DeadlineExceeded {
		return nil, cause
	}
	var (
		taskID string
	)
	res, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		var client scheduler.SchedulerClient
		var err error
		client, err = sc.getSchedulerClient()
		if err != nil {
			return nil, err
		}
		return client.RegisterPeerTask(context.WithValue(ctx, rpc.PickKey{}, &rpc.PickReq{Key: hashKey, Attempt: 2}), ptr, opts...)
	}, 0.2, 2.0, 3, cause)
	if err != nil {
		logger.WithTaskAndPeerID(hashKey, ptr.PeerId).Errorf("retryRegisterPeerTask: register peer task to scheduler failed: %v", err)
		//TODO(zzy987) ensure it can return
		return sc.retryRegisterPeerTask(ctx, hashKey, ptr, err, opts)
	}
	rr := res.(*scheduler.RegisterResult)
	taskID = rr.TaskId
	if taskID != hashKey {
		logger.WithTaskAndPeerID(taskID, ptr.PeerId).Warnf("register peer task correct taskId from %s to %s", hashKey, taskID)
	}
	logger.WithTaskAndPeerID(taskID, ptr.PeerId).
		Infof("register peer task result success url: %s", ptr.Url)
	return rr, nil

}

func (sc *schedulerClient) ReportPieceResult(ctx context.Context, taskID string, ptr *scheduler.PeerTaskRequest, opts ...grpc.CallOption) (PeerPacketStream, error) {
	pps, err := newPeerPacketStream(ctx, sc, taskID, ptr, opts)
	if err != nil {
		return pps, err
	}
	logger.With("peerId", ptr.PeerId, "errMsg", err).Infof("start to report piece result for taskID: %s", taskID)

	// trigger scheduling
	return pps, pps.Send(scheduler.NewZeroPieceResult(taskID, ptr.PeerId))
}

func (sc *schedulerClient) ReportPeerResult(ctx context.Context, pr *scheduler.PeerResult, opts ...grpc.CallOption) error {
	_, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		var client scheduler.SchedulerClient
		var err error
		client, err = sc.getSchedulerClient()
		if err != nil {
			return nil, err
		}
		return client.ReportPeerResult(context.WithValue(ctx, rpc.PickKey{}, &rpc.PickReq{Key: pr.TaskId, Attempt: 1}), pr, opts...)
	}, 0.2, 2.0, 3, nil)
	if err != nil {
		logger.WithTaskAndPeerID(pr.TaskId, pr.PeerId).Errorf("ReportPeerResult: report peer result to scheduler failed: %v", err)
		return sc.retryReportPeerResult(ctx, pr, err, opts)
	}
	return nil
}

func (sc *schedulerClient) retryReportPeerResult(ctx context.Context, pr *scheduler.PeerResult, cause error, opts []grpc.CallOption) (err error) {
	if status.Code(cause) == codes.Canceled || status.Code(cause) == codes.DeadlineExceeded {
		return cause
	}
	var (
		suc  bool
		code base.Code
	)
	_, err = rpc.ExecuteWithRetry(func() (interface{}, error) {
		var client scheduler.SchedulerClient
		client, err = sc.getSchedulerClient()
		if err != nil {
			code = base.Code_ServerUnavailable
			return nil, err
		}
		return client.ReportPeerResult(context.WithValue(ctx, rpc.PickKey{}, &rpc.PickReq{Key: pr.TaskId, Attempt: 2}), pr, opts...)
	}, 0.2, 2.0, 3, nil)
	if err != nil {
		logger.WithTaskAndPeerID(pr.TaskId, pr.PeerId).Errorf("retryReportPeerResult: report peer result to scheduler failed: %v", err)
		//TODO(zzy987) ensure it can return
		return sc.retryReportPeerResult(ctx, pr, cause, opts)
	}

	logger.With("peerId", pr.PeerId, "errMsg", err).
		Infof("report peer result: %t[%d], peer task down result: %t[%d] for taskId: %s, url: %s, length: %d, traffic: %d, cost: %d", suc, int32(code),
			pr.Success, int32(pr.Code), pr.TaskId, pr.Url, pr.ContentLength, pr.Traffic, pr.Cost)

	return
}

func (sc *schedulerClient) LeaveTask(ctx context.Context, pt *scheduler.PeerTarget, opts ...grpc.CallOption) (err error) {
	var (
		suc bool
	)
	defer func() {
		logger.With("peerId", pt.PeerId, "errMsg", err).Infof("leave from task result: %t for taskId: %s, err:%v", suc, pt.TaskId, err)
	}()

	leaveFun := func() (interface{}, error) {
		var client scheduler.SchedulerClient
		client, err = sc.getSchedulerClient()
		if err != nil {
			return nil, err
		}
		return client.LeaveTask(context.WithValue(ctx, rpc.PickKey{}, &rpc.PickReq{Key: pt.TaskId, Attempt: 1}), pt, opts...)
	}
	_, err = rpc.ExecuteWithRetry(leaveFun, 0.2, 2.0, 3, nil)
	if err == nil {
		suc = true
	}
	return
}

var _ SchedulerClient = (*schedulerClient)(nil)
