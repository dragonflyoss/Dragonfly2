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
	"github.com/dragonflyoss/Dragonfly2/pkg/basic/dfnet"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base/common"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler"
	"github.com/dragonflyoss/Dragonfly2/pkg/safe"
	"google.golang.org/grpc"
	"time"
)

// see scheduler.SchedulerClient
type SchedulerClient interface {
	RegisterPeerTask(ctx context.Context, ptr *scheduler.PeerTaskRequest, opts ...grpc.CallOption) (*scheduler.RegisterResult, error)
	// IsMigrating of ptr will be set to true
	ReportPieceResult(ctx context.Context, taskId string, ptr *scheduler.PeerTaskRequest, opts ...grpc.CallOption) (chan<- *scheduler.PieceResult, <-chan *scheduler.PeerPacket, error)
	ReportPeerResult(ctx context.Context, pr *scheduler.PeerResult, opts ...grpc.CallOption) (*base.ResponseState, error)
	LeaveTask(ctx context.Context, pt *scheduler.PeerTarget, opts ...grpc.CallOption) (*base.ResponseState, error)
	Close() error
}

type schedulerClient struct {
	*rpc.Connection
	Client scheduler.SchedulerClient
}

var initClientFunc = func(c *rpc.Connection) {
	sc := c.Ref.(*schedulerClient)
	sc.Client = scheduler.NewSchedulerClient(c.Conn)
	sc.Connection = c
}

func CreateClient(netAddrs []dfnet.NetAddr, opts ...grpc.DialOption) (SchedulerClient, error) {
	if client, err := rpc.BuildClient(&schedulerClient{}, initClientFunc, netAddrs, opts); err != nil {
		return nil, err
	} else {
		return client.(*schedulerClient), nil
	}
}

func (sc *schedulerClient) RegisterPeerTask(ctx context.Context, ptr *scheduler.PeerTaskRequest, opts ...grpc.CallOption) (rr *scheduler.RegisterResult, err error) {
	xc, target, nextNum := sc.GetClientSafely()
	client := xc.(scheduler.SchedulerClient)

	res, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
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
			suc, int32(code), taskId, ptr.Url, ph.Ip, ph.SecurityDomain, ph.Idc, target)

	if err != nil {
		if err := sc.TryMigrate(nextNum, err); err == nil {
			return sc.RegisterPeerTask(ctx, ptr, opts...)
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

	// trigger scheduling
	prc <- scheduler.NewZeroPieceResult(taskId, ptr.PeerId)

	go send(pps, prc, ppc)

	go receive(pps, ppc)

	return prc, ppc, nil
}

func (sc *schedulerClient) ReportPeerResult(ctx context.Context, pr *scheduler.PeerResult, opts ...grpc.CallOption) (rs *base.ResponseState, err error) {
	xc, target, nextNum := sc.GetClientSafely()
	client := xc.(scheduler.SchedulerClient)

	res, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		return client.ReportPeerResult(ctx, pr, opts...)
	}, 0.5, 5.0, 5, nil)

	if err != nil {
		if err := sc.TryMigrate(nextNum, err); err == nil {
			return sc.ReportPeerResult(ctx, pr, opts...)
		}
	}

	if err == nil {
		rs = res.(*base.ResponseState)
	}

	logger.With("peerId", pr.PeerId, "errMsg", err).
		Infof("peer task down result:%t[%d] for taskId:%s,url:%s,scheduler:%s,length:%d,traffic:%d,cost:%d",
			pr.Success, int32(pr.Code), pr.TaskId, pr.Url, target, pr.ContentLength, pr.Traffic, pr.Cost)

	return
}

func (sc *schedulerClient) LeaveTask(ctx context.Context, pt *scheduler.PeerTarget, opts ...grpc.CallOption) (rs *base.ResponseState, err error) {
	xc, target, _ := sc.GetClientSafely()
	client := xc.(scheduler.SchedulerClient)

	res, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		return client.LeaveTask(ctx, pt, opts...)
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
			suc, int32(code), pt.TaskId, target)

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
