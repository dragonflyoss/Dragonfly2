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

	"d7y.io/dragonfly/v2/internal/dfcodes"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/idgen"
	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

	Close() error
}

type schedulerClient struct {
	*rpc.Connection
}

func (sc *schedulerClient) getSchedulerClient() (scheduler.SchedulerClient, string, error) {
	// "scheduler.Scheduler" is the scheduler._Scheduler_serviceDesc.ServiceName
	clientConn, err := sc.Connection.NewConsistentHashClient(fmt.Sprintf("%s:///%s", rpc.SchedulerScheme, "scheduler.Scheduler"))
	if err != nil {
		return nil, "", err
	}
	return scheduler.NewSchedulerClient(clientConn), clientConn.Target(), nil
}

func (sc *schedulerClient) RegisterPeerTask(ctx context.Context, ptr *scheduler.PeerTaskRequest, opts ...grpc.CallOption) (*scheduler.RegisterResult, error) {
	var (
		taskID        string
		schedulerNode string
		res           interface{}
	)
	key := idgen.TaskID(ptr.Url, ptr.UrlMeta)
	logger.WithTaskAndPeerID(key, ptr.PeerId).Infof("generate hash key taskId: %s and start to register peer task for peer_id(%s) url(%s)", key, ptr.PeerId,
		ptr.Url)
	reg := func() (interface{}, error) {
		var client scheduler.SchedulerClient
		var err error
		client, schedulerNode, err = sc.getSchedulerClient()
		if err != nil {
			return nil, err
		}
		return client.RegisterPeerTask(context.WithValue(ctx, rpc.PickKey{}, &rpc.PickReq{Key: key, Attempt: 1}), ptr, opts...)
	}
	res, err := rpc.ExecuteWithRetry(reg, 0.2, 2.0, 3, nil)
	if err != nil {
		logger.WithTaskAndPeerID(key, ptr.PeerId).Errorf("RegisterPeerTask: register peer task to scheduler %s failed: %v", schedulerNode, err)
		return sc.retryRegisterPeerTask(ctx, key, ptr, []string{schedulerNode}, err, opts)
	}
	rr := res.(*scheduler.RegisterResult)
	taskID = rr.TaskId
	if taskID != key {
		logger.WithTaskAndPeerID(taskID, ptr.PeerId).Warnf("register peer task correct taskId from %s to %s", key, taskID)
		//sc.Connection.CorrectKey2NodeRelation(key, taskID)
	}
	logger.WithTaskAndPeerID(taskID, ptr.PeerId).
		Infof("register peer task result success url: %s, scheduler: %s", ptr.Url, schedulerNode)
	return rr, err
}

func (sc *schedulerClient) retryRegisterPeerTask(ctx context.Context, hashKey string, ptr *scheduler.PeerTaskRequest, exclusiveNodes []string, cause error,
	opts []grpc.CallOption) (*scheduler.RegisterResult, error) {
	if status.Code(cause) == codes.Canceled || status.Code(cause) == codes.DeadlineExceeded {
		return nil, cause
	}
	var (
		taskID        string
		schedulerNode string
	)
	res, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		var client scheduler.SchedulerClient
		var err error
		client, schedulerNode, err = sc.getSchedulerClient()
		if err != nil {
			return nil, err
		}
		return client.RegisterPeerTask(context.WithValue(ctx, rpc.PickKey{}, &rpc.PickReq{Key: hashKey, Attempt: 2}), ptr, opts...)
	}, 0.2, 2.0, 3, cause)
	if err != nil {
		logger.WithTaskAndPeerID(hashKey, ptr.PeerId).Errorf("retryRegisterPeerTask: register peer task to scheduler %s failed: %v", schedulerNode, err)
		//TODO(zzy987) ensure it can return
		return sc.retryRegisterPeerTask(ctx, hashKey, ptr, exclusiveNodes, err, opts)
	}
	rr := res.(*scheduler.RegisterResult)
	taskID = rr.TaskId
	if taskID != hashKey {
		logger.WithTaskAndPeerID(taskID, ptr.PeerId).Warnf("register peer task correct taskId from %s to %s", hashKey, taskID)
		//sc.Connection.CorrectKey2NodeRelation(hashKey, taskID)
	}
	logger.WithTaskAndPeerID(taskID, ptr.PeerId).
		Infof("register peer task result success url: %s, scheduler: %s", ptr.Url, schedulerNode)
	return rr, nil

}

func (sc *schedulerClient) ReportPieceResult(ctx context.Context, taskID string, ptr *scheduler.PeerTaskRequest, opts ...grpc.CallOption) (PeerPacketStream, error) {
	pps, err := newPeerPacketStream(ctx, sc, taskID, ptr, opts)
	if err != nil {
		return pps, err
	}
	logger.With("peerId", ptr.PeerId, "errMsg", err).Infof("start to report piece result for taskID: %s", taskID)

	// trigger scheduling
	pps.Send(scheduler.NewZeroPieceResult(taskID, ptr.PeerId))
	return pps, err
}

func (sc *schedulerClient) ReportPeerResult(ctx context.Context, pr *scheduler.PeerResult, opts ...grpc.CallOption) error {
	var (
		schedulerNode string
	)
	_, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		var client scheduler.SchedulerClient
		var err error
		client, schedulerNode, err = sc.getSchedulerClient()
		if err != nil {
			return nil, err
		}
		return client.ReportPeerResult(context.WithValue(ctx, rpc.PickKey{}, &rpc.PickReq{Key: pr.TaskId, Attempt: 1}), pr, opts...)
	}, 0.2, 2.0, 3, nil)
	if err != nil {
		logger.WithTaskAndPeerID(pr.TaskId, pr.PeerId).Errorf("ReportPeerResult: report peer result to scheduler %s failed: %v", schedulerNode, err)
		return sc.retryReportPeerResult(ctx, pr, []string{schedulerNode}, err, opts)
	}
	return nil
}

func (sc *schedulerClient) retryReportPeerResult(ctx context.Context, pr *scheduler.PeerResult, exclusiveNodes []string,
	cause error, opts []grpc.CallOption) (err error) {
	if status.Code(cause) == codes.Canceled || status.Code(cause) == codes.DeadlineExceeded {
		return cause
	}
	var (
		schedulerNode string
		suc           bool
		code          base.Code
	)
	_, err = rpc.ExecuteWithRetry(func() (interface{}, error) {
		var client scheduler.SchedulerClient
		client, schedulerNode, err = sc.getSchedulerClient()
		if err != nil {
			code = dfcodes.ServerUnavailable
			return nil, err
		}
		return client.ReportPeerResult(context.WithValue(ctx, rpc.PickKey{}, &rpc.PickReq{Key: pr.TaskId, Attempt: 2}), pr, opts...)
	}, 0.2, 2.0, 3, nil)
	if err != nil {
		logger.WithTaskAndPeerID(pr.TaskId, pr.PeerId).Errorf("retryReportPeerResult: report peer result to scheduler %s failed: %v", schedulerNode, err)
		//TODO(zzy987) ensure it can return
		return sc.retryReportPeerResult(ctx, pr, exclusiveNodes, cause, opts)
	}

	logger.With("peerId", pr.PeerId, "errMsg", err).
		Infof("report peer result: %t[%d], peer task down result: %t[%d] for taskId: %s, url: %s, scheduler: %s, length: %d, traffic: %d, cost: %d", suc, int32(code),
			pr.Success, int32(pr.Code), pr.TaskId, pr.Url, schedulerNode, pr.ContentLength, pr.Traffic, pr.Cost)

	return
}

func (sc *schedulerClient) LeaveTask(ctx context.Context, pt *scheduler.PeerTarget, opts ...grpc.CallOption) (err error) {
	var (
		schedulerNode string
		suc           bool
	)
	defer func() {
		logger.With("peerId", pt.PeerId, "errMsg", err).Infof("leave from task result: %t for taskId: %s, scheduler server node: %s, err:%v", suc, pt.TaskId,
			schedulerNode, err)
	}()

	leaveFun := func() (interface{}, error) {
		var client scheduler.SchedulerClient
		client, schedulerNode, err = sc.getSchedulerClient()
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
