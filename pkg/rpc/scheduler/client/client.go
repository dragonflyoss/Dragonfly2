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
	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/base/common"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/pkg/safe"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"sync"
	"time"
)

func GetClient() (SchedulerClient, error) {
	// 从本地文件/manager读取addrs
	return sc, nil
}

var sc *schedulerClient

var once sync.Once

func GetClientByAddr(addrs []dfnet.NetAddr) (SchedulerClient, error) {
	once.Do(func() {
		sc = &schedulerClient{
			rpc.NewConnection("scheduler", make([]dfnet.NetAddr, 0)),
		}
	})
	if len(addrs) == 0 {
		return nil, errors.New("address list of cdn is empty")
	}
	err := sc.Connection.AddNodes(addrs)
	if err != nil {
		return nil, err
	}
	return sc, nil
}

//func newSchedulerClient(addrs []dfnet.NetAddr, opts ...grpc.DialOption) (SchedulerClient, error) {
//	if len(addrs) == 0 {
//		return nil, errors.New("address list of cdn is empty")
//	}
//	return &schedulerClient{
//		rpc.NewConnection(addrs, opts...),
//	}, nil
//}

// see scheduler.SchedulerClient
type SchedulerClient interface {

	RegisterPeerTask(ctx context.Context, ptr *scheduler.PeerTaskRequest, opts ...grpc.CallOption) (*scheduler.RegisterResult, error)
	// IsMigrating of ptr will be set to true
	ReportPieceResult(ctx context.Context, taskId string, ptr *scheduler.PeerTaskRequest, opts ...grpc.CallOption) (chan<- *scheduler.PieceResult, <-chan *scheduler.PeerPacket, error)

	ReportPeerResult(ctx context.Context, pr *scheduler.PeerResult, opts ...grpc.CallOption) (*base.ResponseState, error)

	LeaveTask(ctx context.Context, pt *scheduler.PeerTarget, opts ...grpc.CallOption) (*base.ResponseState, error)
}

type schedulerClient struct {
	*rpc.Connection
}

func (sc *schedulerClient) getSchedulerClient(key string) (scheduler.SchedulerClient, error){
	if clientConn, err := sc.Connection.GetClientConn(key); err != nil {
		return nil, err
	} else {
		return scheduler.NewSchedulerClient(clientConn), nil
	}
}

func (sc *schedulerClient) RegisterPeerTask(ctx context.Context, ptr *scheduler.PeerTaskRequest, opts ...grpc.CallOption) (rr *scheduler.RegisterResult, err error) {
	return sc.doRegisterPeerTask(ctx, ptr, []string{}, opts)
}

func (sc *schedulerClient) doRegisterPeerTask(ctx context.Context, ptr *scheduler.PeerTaskRequest, exclusiveNodes []string, opts []grpc.CallOption) (rr *scheduler.RegisterResult, err error) {
	key := fmt.Sprintf("%s,%s,%s", ptr.Url, ptr.Filter, ptr.BizId)
	res, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		client, err := sc.getSchedulerClient(key)
		if err != nil {
			return nil, err
		}
		return client.RegisterPeerTask(ctx, ptr, opts...)
	}, 0.5, 5.0, 5, nil)

	var taskId = "unknown"
	var suc bool
	var code base.Code
	if err == nil {
		rr = res.(*scheduler.RegisterResult)
		taskId = rr.TaskId
		suc = rr.State.Success
		code = rr.State.Code
	}

	ph := ptr.PeerHost
	logger.With("peerId", ptr.PeerId, "errMsg", err).
		Infof("register peer task result:%t[%d] for taskId:%s,url:%s,peerIp:%s,securityDomain:%s,idc:%s,scheduler:%s",
			suc, int32(code), taskId, ptr.Url, ph.Ip, ph.SecurityDomain, ph.Idc, sc.GetServerNode(key))

	if err != nil {
		if preNode, err := sc.TryMigrate(key, err, exclusiveNodes); err == nil {
			exclusiveNodes = append(exclusiveNodes, preNode)
			return sc.doRegisterPeerTask(ctx, ptr, exclusiveNodes, opts)
		}
	}

	return
}

func (sc *schedulerClient) ReportPieceResult(ctx context.Context, taskId string, ptr *scheduler.PeerTaskRequest, opts ...grpc.CallOption) (chan<- *scheduler.PieceResult, <-chan *scheduler.PeerPacket, error) {
	prc := make(chan *scheduler.PieceResult, 4)
	ppc := make(chan *scheduler.PeerPacket, 4)

	pps, err := newPeerPacketStream(sc, ctx, taskId, ptr, opts, prc)

	logger.With("peerId", ptr.PeerId, "errMsg", err).
		Infof("start to report piece result for taskId:%s", taskId)

	if err != nil {
		return nil, nil, err
	}

	go send(pps, prc, ppc)

	go receive(pps, ppc)

	// trigger scheduling
	prc <- scheduler.NewZeroPieceResult(taskId, ptr.PeerId)

	return prc, ppc, nil
}

func (sc *schedulerClient) ReportPeerResult(ctx context.Context, pr *scheduler.PeerResult, opts ...grpc.CallOption) (rs *base.ResponseState, err error) {
	return sc.doReportPeerResult(ctx, pr, []string{}, opts)
}

func (sc *schedulerClient) doReportPeerResult(ctx context.Context, pr *scheduler.PeerResult, exclusiveNodes []string, opts []grpc.CallOption) (rs *base.ResponseState, err error) {
	res, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		if client, err := sc.getSchedulerClient(pr.TaskId); err != nil {
			return nil, err
		} else {
			return client.ReportPeerResult(ctx, pr, opts...)
		}
	}, 0.5, 5.0, 5, nil)

	if err != nil {
		if preNode, err := sc.TryMigrate(pr.TaskId, err, exclusiveNodes); err == nil {
			exclusiveNodes = append(exclusiveNodes, preNode)
			return sc.doReportPeerResult(ctx, pr, exclusiveNodes, opts)
		}
	}

	if err == nil {
		rs = res.(*base.ResponseState)
	}

	logger.With("peerId", pr.PeerId, "errMsg", err).
		Infof("peer task down result:%t[%d] for taskId:%s,url:%s,scheduler:%s,length:%d,traffic:%d,cost:%d",
			pr.Success, int32(pr.Code), pr.TaskId, pr.Url, sc.GetServerNode(pr.TaskId), pr.ContentLength, pr.Traffic,
			pr.Cost)

	return
}

func (sc *schedulerClient) LeaveTask(ctx context.Context, pt *scheduler.PeerTarget, opts ...grpc.CallOption) (rs *base.ResponseState, err error) {
	res, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		if client, err := sc.getSchedulerClient(pt.TaskId); err != nil {
			return nil, err
		} else {
			return client.LeaveTask(ctx, pt, opts...)
		}
	}, 0.5, 5.0, 3, nil)

	var suc bool
	var code base.Code
	if err == nil {
		rs = res.(*base.ResponseState)
		suc = rs.Success
		code = rs.Code
	}

	logger.With("peerId", pt.PeerId, "errMsg", err).
		Infof("leave from task result:%t[%d] for taskId:%s,scheduler:%s",
			suc, int32(code), pt.TaskId, sc.GetServerNode(pt.TaskId))

	return
}

func receive(stream *peerPacketStream, ppc chan *scheduler.PeerPacket) {
	safe.Call(func() {
		for {
			if peerPacket, err := stream.recv(); err == nil {
				ppc <- peerPacket
			} else {
				// return error and check ppc
				ppc <- common.NewResWithErr(peerPacket, err).(*scheduler.PeerPacket)
				time.Sleep(200 * time.Millisecond)
			}
		}
	})
}

// no send no receive
func send(stream *peerPacketStream, prc chan *scheduler.PieceResult, ppc chan *scheduler.PeerPacket) {
	safe.Call(func() {
		defer close(ppc)
		defer close(prc)
		defer stream.closeSend()

		for v := range prc {
			if err := stream.send(v); err != nil {
				return
			} else if v.PieceNum == common.EndOfPiece {
				return
			}
		}
	})
}