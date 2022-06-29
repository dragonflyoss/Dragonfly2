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

//go:generate mockgen -destination mocks/client_mock.go -source client.go -package mocks

package client

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/dfnet"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/base/common"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
)

// NewBeginOfPiece creates begin of piece.
func NewBeginOfPiece(taskID, peerID string) *scheduler.PieceResult {
	return &scheduler.PieceResult{
		TaskId: taskID,
		SrcPid: peerID,
		PieceInfo: &base.PieceInfo{
			PieceNum: common.BeginOfPiece,
		},
	}
}

// NewBeginOfPiece creates end of piece.
func NewEndOfPiece(taskID, peerID string, finishedCount int32) *scheduler.PieceResult {
	return &scheduler.PieceResult{
		TaskId:        taskID,
		SrcPid:        peerID,
		FinishedCount: finishedCount,
		PieceInfo: &base.PieceInfo{
			PieceNum: common.EndOfPiece,
		},
	}
}

// GetClientByAddr gets scheduler clients with net addresses.
func GetClientByAddr(addrs []dfnet.NetAddr, opts ...grpc.DialOption) (Client, error) {
	if len(addrs) == 0 {
		return nil, errors.New("scheduler addresses are empty")
	}

	logger.Infof("scheduler addresses: %s", addrs)
	return &client{
		rpc.NewConnection(context.Background(), "scheduler", addrs, []rpc.ConnOption{
			rpc.WithConnExpireTime(60 * time.Second),
			rpc.WithDialOption(opts),
		}),
	}, nil
}

// Client is the interface for grpc client.
type Client interface {
	// RegisterPeerTask registers a peer into task.
	RegisterPeerTask(context.Context, *scheduler.PeerTaskRequest, ...grpc.CallOption) (*scheduler.RegisterResult, error)

	// ReportPieceResult reports piece results and receives peer packets.
	ReportPieceResult(context.Context, *scheduler.PeerTaskRequest, ...grpc.CallOption) (scheduler.Scheduler_ReportPieceResultClient, error)

	// ReportPeerResult reports downloading result for the peer.
	ReportPeerResult(context.Context, *scheduler.PeerResult, ...grpc.CallOption) error

	// LeaveTask makes the peer leaving from task.
	LeaveTask(context.Context, *scheduler.PeerTarget, ...grpc.CallOption) error

	// Checks if any peer has the given task.
	StatTask(context.Context, *scheduler.StatTaskRequest, ...grpc.CallOption) (*scheduler.Task, error)

	// A peer announces that it has the announced task to other peers.
	AnnounceTask(context.Context, *scheduler.AnnounceTaskRequest, ...grpc.CallOption) error

	// Update grpc addresses.
	UpdateState([]dfnet.NetAddr)

	// Get grpc addresses.
	GetState() []dfnet.NetAddr

	// Close grpc service.
	Close() error
}

// client provides scheduler grpc function.
type client struct {
	*rpc.Connection
}

// getClient gets scheduler client with hashkey.
func (sc *client) getClient(key string, stick bool) (scheduler.SchedulerClient, string, error) {
	clientConn, err := sc.Connection.GetClientConn(key, stick)
	if err != nil {
		return nil, "", err
	}

	return scheduler.NewSchedulerClient(clientConn), clientConn.Target(), nil
}

// RegisterPeerTask registers a peer into task.
func (sc *client) RegisterPeerTask(ctx context.Context, req *scheduler.PeerTaskRequest, opts ...grpc.CallOption) (*scheduler.RegisterResult, error) {
	// Generate task id.
	client, target, err := sc.getClient(req.TaskId, false)
	if err != nil {
		return nil, err
	}

	logger.WithTaskAndPeerID(req.TaskId, req.PeerId).Infof("register peer task with %s request: %#v %#v %#v", target, req, req.UrlMeta, req.HostLoad)
	resp, err := client.RegisterPeerTask(ctx, req, opts...)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

// ReportPieceResult reports piece results and receives peer packets.
func (sc *client) ReportPieceResult(ctx context.Context, req *scheduler.PeerTaskRequest, opts ...grpc.CallOption) (scheduler.Scheduler_ReportPieceResultClient, error) {
	client, target, err := sc.getClient(req.TaskId, false)
	if err != nil {
		return nil, err
	}

	stream, err := client.ReportPieceResult(ctx, opts...)
	if err != nil {
		return nil, err
	}

	logger.WithTaskAndPeerID(req.TaskId, req.PeerId).Infof("report piece result with %s request: %#v", target, req)
	return stream, stream.Send(NewBeginOfPiece(req.TaskId, req.PeerId))
}

// ReportPeerResult reports downloading result for the peer.
func (sc *client) ReportPeerResult(ctx context.Context, req *scheduler.PeerResult, opts ...grpc.CallOption) error {
	client, target, err := sc.getClient(req.TaskId, false)
	if err != nil {
		return err
	}

	logger.WithTaskAndPeerID(req.TaskId, req.PeerId).Infof("report peer result with %s request: %#v", target, req)
	if _, err := client.ReportPeerResult(ctx, req, opts...); err != nil {
		return err
	}

	return nil
}

// LeaveTask makes the peer leaving from task.
func (sc *client) LeaveTask(ctx context.Context, req *scheduler.PeerTarget, opts ...grpc.CallOption) error {
	client, target, err := sc.getClient(req.TaskId, false)
	if err != nil {
		return err
	}

	logger.WithTaskAndPeerID(req.TaskId, req.PeerId).Infof("leave task with %s request: %#v", target, req)
	if _, err := client.LeaveTask(ctx, req, opts...); err != nil {
		return err
	}

	return nil
}

// Checks if any peer has the given task.
func (sc *client) StatTask(ctx context.Context, req *scheduler.StatTaskRequest, opts ...grpc.CallOption) (*scheduler.Task, error) {
	client, target, err := sc.getClient(req.TaskId, false)
	if err != nil {
		return nil, err
	}

	logger.WithTaskID(req.TaskId).Infof("stat task with %s request: %#v", target, req)
	resp, err := client.StatTask(ctx, req, opts...)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

// A peer announces that it has the announced task to other peers.
func (sc *client) AnnounceTask(ctx context.Context, req *scheduler.AnnounceTaskRequest, opts ...grpc.CallOption) error {
	client, target, err := sc.getClient(req.TaskId, false)
	if err != nil {
		return err
	}

	logger.WithTaskID(req.TaskId).Infof("announce task with %s request: %#v", target, req)
	if _, err := client.AnnounceTask(ctx, req, opts...); err != nil {
		return err
	}

	return nil
}
