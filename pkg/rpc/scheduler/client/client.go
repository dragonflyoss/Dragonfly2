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
	"time"

	"github.com/pkg/errors"
	"google.golang.org/grpc"

	"d7y.io/dragonfly/v2/internal/dfcodes"
	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/idgen"
	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
)

func GetClientByAddr(addrs []dfnet.NetAddr, opts ...grpc.DialOption) (SchedulerClient, error) {
	if len(addrs) == 0 {
		return nil, errors.New("address list of scheduler is empty")
	}
	sc := &schedulerClient{
		rpc.NewConnection(context.Background(), "scheduler-static", addrs, []rpc.ConnOption{
			rpc.WithConnExpireTime(30 * time.Minute),
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

func (sc *schedulerClient) getSchedulerClient(key string, stick bool) (scheduler.SchedulerClient, string, error) {
	clientConn, err := sc.Connection.GetClientConn(key, stick)
	if err != nil {
		return nil, "", err
	}
	return scheduler.NewSchedulerClient(clientConn), clientConn.Target(), nil
}

func (sc *schedulerClient) RegisterPeerTask(ctx context.Context, ptr *scheduler.PeerTaskRequest, opts ...grpc.CallOption) (rr *scheduler.RegisterResult,
	err error) {
	return sc.doRegisterPeerTask(ctx, ptr, []string{}, opts)
}

func (sc *schedulerClient) doRegisterPeerTask(ctx context.Context, ptr *scheduler.PeerTaskRequest, exclusiveNodes []string,
	opts []grpc.CallOption) (rr *scheduler.RegisterResult, err error) {
	var (
		taskID        string
		code          base.Code
		schedulerNode string
		res           interface{}
	)
	key := idgen.TaskID(ptr.Url, ptr.UrlMeta)
	logger.WithTaskAndPeerID(key, ptr.PeerId).Infof("generate hash key taskId: %s and start to register peer task for peer_id(%s) url(%s)", key, ptr.PeerId,
		ptr.Url)
	reg := func() (interface{}, error) {
		var client scheduler.SchedulerClient
		client, schedulerNode, err = sc.getSchedulerClient(key, false)
		if err != nil {
			code = dfcodes.ServerUnavailable
			return nil, err
		}
		return client.RegisterPeerTask(ctx, ptr, opts...)
	}
	res, err = rpc.ExecuteWithRetry(reg, 0.5, 5.0, 5, nil)
	if err == nil {
		rr = res.(*scheduler.RegisterResult)
		taskID = rr.TaskId
		code = dfcodes.Success
		if taskID != key {
			logger.WithTaskAndPeerID(taskID, ptr.PeerId).Warnf("register peer task correct taskId from %s to %s", key, taskID)
			sc.Connection.CorrectKey2NodeRelation(key, taskID)
		}
		logger.With("peerId", ptr.PeerId).
			Infof("register peer task result success for taskId: %s, url: %s, scheduler: %s",
				taskID, ptr.Url, schedulerNode)
		return
	}

	if de, ok := err.(*dferrors.DfError); ok {
		code = de.Code
	}
	logger.With("peerId", ptr.PeerId, "errMsg", err).
		Errorf("register peer task result failed, code: [%d] for taskId: %s, url: %s, scheduler: %s",
			int32(code), taskID, ptr.Url, schedulerNode)

	// previous schedule failed, report peer task to free load and other resources
	var client scheduler.SchedulerClient
	client, schedulerNode, err = sc.getSchedulerClient(key, false)
	if err != nil {
		logger.With("peerId", ptr.PeerId, "errMsg", err).Errorf("get scheduler client failed")
	} else {
		_, e := client.ReportPeerResult(
			context.Background(),
			&scheduler.PeerResult{
				TaskId:         taskID,
				PeerId:         ptr.PeerId,
				SrcIp:          ptr.PeerHost.Ip,
				SecurityDomain: ptr.PeerHost.SecurityDomain,
				Idc:            ptr.PeerHost.Idc,
				Url:            ptr.Url,
				ContentLength:  -1,
				Traffic:        -1,
				Cost:           0,
				Success:        false,
				Code:           dfcodes.UnknownError,
			})
		logger.With("peerId", ptr.PeerId, "errMsg", e).Warnf("report failed peer result")
	}

	var preNode string
	if preNode, err = sc.TryMigrate(key, err, exclusiveNodes); err == nil {
		exclusiveNodes = append(exclusiveNodes, preNode)
		return sc.doRegisterPeerTask(ctx, ptr, exclusiveNodes, opts)
	}
	return
}

func (sc *schedulerClient) ReportPieceResult(ctx context.Context, taskID string, ptr *scheduler.PeerTaskRequest, opts ...grpc.CallOption) (PeerPacketStream, error) {
	pps, err := newPeerPacketStream(ctx, sc, taskID, ptr, opts)

	logger.With("peerId", ptr.PeerId, "errMsg", err).Infof("start to report piece result for taskID: %s", taskID)

	// trigger scheduling
	pps.Send(scheduler.NewZeroPieceResult(taskID, ptr.PeerId))
	return pps, err
}

func (sc *schedulerClient) ReportPeerResult(ctx context.Context, pr *scheduler.PeerResult, opts ...grpc.CallOption) error {
	return sc.doReportPeerResult(ctx, pr, []string{}, opts)
}

func (sc *schedulerClient) doReportPeerResult(ctx context.Context, pr *scheduler.PeerResult, exclusiveNodes []string, opts []grpc.CallOption) (err error) {
	var (
		schedulerNode string
		suc           bool
		code          base.Code
	)

	_, err = rpc.ExecuteWithRetry(func() (interface{}, error) {
		var client scheduler.SchedulerClient
		client, schedulerNode, err = sc.getSchedulerClient(pr.TaskId, true)
		if err != nil {
			code = dfcodes.ServerUnavailable
			return nil, err
		}
		return client.ReportPeerResult(ctx, pr, opts...)
	}, 0.5, 5.0, 5, nil)
	if err == nil {
		suc = true
		code = dfcodes.Success
	}

	logger.With("peerId", pr.PeerId, "errMsg", err).
		Infof("report peer result: %t[%d], peer task down result: %t[%d] for taskId: %s, url: %s, scheduler: %s, length: %d, traffic: %d, cost: %d", suc, int32(code),
			pr.Success, int32(pr.Code), pr.TaskId, pr.Url, schedulerNode, pr.ContentLength, pr.Traffic, pr.Cost)

	if err != nil {
		var preNode string
		if preNode, err = sc.TryMigrate(pr.TaskId, err, exclusiveNodes); err == nil {
			exclusiveNodes = append(exclusiveNodes, preNode)
			return sc.doReportPeerResult(ctx, pr, exclusiveNodes, opts)
		}
	}

	return
}

func (sc *schedulerClient) LeaveTask(ctx context.Context, pt *scheduler.PeerTarget, opts ...grpc.CallOption) (err error) {
	var (
		schedulerNode string
		suc           bool
	)
	defer func() {
		logger.With("peerId", pt.PeerId, "errMsg", err).Infof("leave from task result: %t for taskId: %s, scheduler: %s", suc, pt.TaskId, schedulerNode)
	}()

	_, err = rpc.ExecuteWithRetry(func() (interface{}, error) {
		var client scheduler.SchedulerClient
		client, schedulerNode, err = sc.getSchedulerClient(pt.TaskId, true)
		if err != nil {
			return nil, err
		}
		return client.LeaveTask(ctx, pt, opts...)
	}, 0.5, 5.0, 3, nil)
	if err == nil {
		suc = true
	}

	return
}

var _ SchedulerClient = (*schedulerClient)(nil)
