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
	"github.com/dragonflyoss/Dragonfly2/pkg/basic"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler"
	"github.com/dragonflyoss/Dragonfly2/pkg/safe"
	"google.golang.org/grpc"
)

type SchedulerClient interface {
	// RegisterPeerTask registers a peer into one task
	// and returns a peer packet immediately if task resource is enough.
	RegisterPeerTask(ctx context.Context, ptr *scheduler.PeerTaskRequest, opts ...grpc.CallOption) (*scheduler.PeerPacket, error)
	// ReportPieceResult reports piece results and receives peer packets.
	// when migrating to another scheduler,
	// it will send the last piece result to the new scheduler.
	//
	// sending to chan must bind a recover, it is recommended that using safe.Call wraps these func.
	ReportPieceResult(ctx context.Context, taskId string, ptr *scheduler.PeerTaskRequest, opts ...grpc.CallOption) (chan<- *scheduler.PieceResult, <-chan *scheduler.PeerPacket, error)
	// ReportPeerResult reports downloading result for the peer task.
	ReportPeerResult(ctx context.Context, pr *scheduler.PeerResult, opts ...grpc.CallOption) (*base.ResponseState, error)
	// LeaveTask makes the peer leaving from scheduling overlay for the task.
	LeaveTask(ctx context.Context, pt *scheduler.PeerTarget, opts ...grpc.CallOption) (*base.ResponseState, error)
	// close the client
	Close() error
}

type schedulerClient struct {
	*rpc.Connection
	lastResult *scheduler.PieceResult
	Client     scheduler.SchedulerClient
}

// init client info excepting connection
var initClientFunc = func(c *rpc.Connection) {
	sc := c.Ref.(*schedulerClient)
	sc.Client = scheduler.NewSchedulerClient(c.Conn)
	sc.Connection = c
}

// netAddrs are used to connect and migrate
func CreateClient(netAddrs []basic.NetAddr) (SchedulerClient, error) {
	if client, err := rpc.BuildClient(&schedulerClient{}, initClientFunc, netAddrs); err != nil {
		return nil, err
	} else {
		return client.(*schedulerClient), nil
	}
}

func (sc *schedulerClient) RegisterPeerTask(ctx context.Context, ptr *scheduler.PeerTaskRequest, opts ...grpc.CallOption) (pp *scheduler.PeerPacket, err error) {
	xc, target, nextNum := sc.GetClientSafely()
	client := xc.(scheduler.SchedulerClient)

	res, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		return client.RegisterPeerTask(ctx, ptr, opts...)
	}, 0.5, 5.0, 5)

	var taskId = "unknown"
	var suc bool
	var code base.Code
	if err == nil {
		pp = res.(*scheduler.PeerPacket)
		taskId = pp.TaskId
		suc = pp.State.Success
		code = pp.State.Code
	}

	ph := ptr.PeerHost
	logger.With("peerId", ptr.PeerId).Infof("register peer task result:%t[%d] for taskId:%s,url:%s,peerIp:%s,securityDomain:%s,idc:%s,scheduler:%s",
		suc, int(code), taskId, ptr.Url, ph.Ip, ph.SecurityDomain, ph.Idc, target)

	if err != nil {
		if err = sc.TryMigrate(nextNum, err); err == nil {
			return sc.RegisterPeerTask(ctx, ptr, opts...)
		}
	}

	return
}

// push piece result and pull piece tasks
func (sc *schedulerClient) ReportPieceResult(ctx context.Context, taskId string, ptr *scheduler.PeerTaskRequest, opts ...grpc.CallOption) (chan<- *scheduler.PieceResult, <-chan *scheduler.PeerPacket, error) {
	prc := make(chan *scheduler.PieceResult, 4)
	ppc := make(chan *scheduler.PiecePackage, 4)

	pts, err := newPieceTaskStream(sc, ctx, taskId, ptr, opts, prc)
	if err != nil {
		return nil, nil, err
	}

	go send(pts, prc)

	go receive(pts, ppc, prc)

	return prc, ppc, nil
}

// report whole file's downloading result for the peer
func (sc *schedulerClient) ReportPeerResult(ctx context.Context, pr *scheduler.PeerResult, opts ...grpc.CallOption) (rs *base.ResponseState, err error) {
	xc, target, nextNum := sc.GetClientSafely()
	client := xc.(scheduler.SchedulerClient)

	res, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		return client.ReportPeerResult(ctx, pr, opts...)
	}, 0.5, 5.0, 5)

	if err != nil {
		if err = sc.TryMigrate(nextNum, err); err == nil {
			return sc.ReportPeerResult(ctx, pr, opts...)
		}
	}

	if err == nil {
		rs = res.(*base.ResponseState)
	}

	logger.With("peerId", pr.PeerId).Infof("peer task down result:%t[%d] for taskId:%s,url:%s,scheduler:%s,length:%d,traffic:%d,cost:%d",
		pr.Success, int(pr.ErrorCode), pr.TaskId, pr.Url, target, pr.ContentLength, pr.Traffic, pr.Cost)

	return
}

// make peer leaving from scheduling overlay
func (sc *schedulerClient) LeaveTask(ctx context.Context, pt *scheduler.PeerTarget, opts ...grpc.CallOption) (rs *base.ResponseState, err error) {
	xc, target, _ := sc.GetClientSafely()
	client := xc.(scheduler.SchedulerClient)

	res, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		return client.LeaveTask(ctx, pt, opts...)
	}, 0.5, 5.0, 5)

	var suc bool
	var code base.Code
	if err == nil {
		rs = res.(*base.ResponseState)
		suc = rs.Success
		code = rs.Code
	}

	logger.With("peerId", pt.PeerId).Infof("leave from task result:%t[%d] for taskId:%s,scheduler:%s", suc, int(code), pt.TaskId, target)

	return
}

// receiver also finishes sender
func receive(stream *pieceTaskStream, ppc chan *scheduler.PiecePackage, prc chan *scheduler.PieceResult) {
	safe.Call(func() {
		defer close(prc)
		defer close(ppc)

		for {
			piecePackage, err := stream.recv()
			if err == nil {
				ppc <- piecePackage
				if piecePackage.Done {
					return
				}
			} else {
				ppc <- base.NewResWithErr(piecePackage, err).(*scheduler.PiecePackage)
				return
			}
		}
	})
}

func send(stream *pieceTaskStream, prc chan *scheduler.PieceResult) {
	safe.Call(func() {
		defer stream.closeSend()

		for v := range prc {
			_ = stream.send(v)
		}
	})
}
