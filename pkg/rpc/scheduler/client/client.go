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
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"

	commonv1 "d7y.io/api/pkg/apis/common/v1"
	schedulerv1 "d7y.io/api/pkg/apis/scheduler/v1"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/consistent"
	"d7y.io/dragonfly/v2/pkg/rpc/common"
)

const (
	backoffBaseDelay  = 1 * time.Second
	backoffMultiplier = 1.2
	backoffJitter     = 0.2
	backoffMaxDelay   = 120 * time.Second
	minConnectTime    = 3 * time.Second //fast fail, leave time to try other scheduler
)

// NewBeginOfPiece creates begin of piece.
func NewBeginOfPiece(taskID, peerID string) *schedulerv1.PieceResult {
	return &schedulerv1.PieceResult{
		TaskId: taskID,
		SrcPid: peerID,
		PieceInfo: &commonv1.PieceInfo{
			PieceNum: common.BeginOfPiece,
		},
	}
}

// NewBeginOfPiece creates end of piece.
func NewEndOfPiece(taskID, peerID string, finishedCount int32) *schedulerv1.PieceResult {
	return &schedulerv1.PieceResult{
		TaskId:        taskID,
		SrcPid:        peerID,
		FinishedCount: finishedCount,
		PieceInfo: &commonv1.PieceInfo{
			PieceNum: common.EndOfPiece,
		},
	}
}

// GetClient get scheduler clients using resolver and balancer
func GetClient(opts ...grpc.DialOption) (Client, error) {
	opts = append(opts,
		grpc.WithDefaultServiceConfig(consistent.BalancerServiceConfig),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  backoffBaseDelay,
				Multiplier: backoffMultiplier,
				Jitter:     backoffJitter,
				MaxDelay:   backoffMaxDelay,
			},
			MinConnectTimeout: minConnectTime,
		}),
		grpc.WithStreamInterceptor(grpc_middleware.ChainStreamClient(
			grpc_prometheus.StreamClientInterceptor,
			grpc_zap.StreamClientInterceptor(logger.GrpcLogger.Desugar()),
		)))

	conn, err := grpc.Dial(
		consistent.DragonflyScheme+"://"+consistent.DragonflyHostScheduler,
		opts...,
	)

	return &client{
		conn,
		schedulerv1.NewSchedulerClient(conn),
	}, err
}

// Client is the interface for grpc client.
type Client interface {
	// RegisterPeerTask registers a peer into task.
	RegisterPeerTask(context.Context, *schedulerv1.PeerTaskRequest, ...grpc.CallOption) (*schedulerv1.RegisterResult, error)

	// ReportPieceResult reports piece results and receives peer packets.
	ReportPieceResult(context.Context, *schedulerv1.PeerTaskRequest, ...grpc.CallOption) (schedulerv1.Scheduler_ReportPieceResultClient, error)

	// ReportPeerResult reports downloading result for the peer.
	ReportPeerResult(context.Context, *schedulerv1.PeerResult, ...grpc.CallOption) error

	// LeaveTask makes the peer leaving from task.
	LeaveTask(context.Context, *schedulerv1.PeerTarget, ...grpc.CallOption) error

	// Checks if any peer has the given task.
	StatTask(context.Context, *schedulerv1.StatTaskRequest, ...grpc.CallOption) (*schedulerv1.Task, error)

	// A peer announces that it has the announced task to other peers.
	AnnounceTask(context.Context, *schedulerv1.AnnounceTaskRequest, ...grpc.CallOption) error

	// Close grpc service.
	Close() error
}

// client provides scheduler grpc function.
type client struct {
	*grpc.ClientConn
	schedulerv1.SchedulerClient
}

// RegisterPeerTask registers a peer into task.
func (sc *client) RegisterPeerTask(ctx context.Context, req *schedulerv1.PeerTaskRequest, opts ...grpc.CallOption) (*schedulerv1.RegisterResult, error) {

	ctx = context.WithValue(ctx, consistent.ConsistentHashKey, req.TaskId)
	resp, err := sc.SchedulerClient.RegisterPeerTask(ctx, req, opts...)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

// ReportPieceResult reports piece results and receives peer packets.
func (sc *client) ReportPieceResult(ctx context.Context, req *schedulerv1.PeerTaskRequest, opts ...grpc.CallOption) (schedulerv1.Scheduler_ReportPieceResultClient, error) {

	taskCtx := context.WithValue(ctx, consistent.ConsistentHashKey, req.TaskId)
	stream, err := sc.SchedulerClient.ReportPieceResult(taskCtx, opts...)
	if err != nil {
		return nil, err
	}

	return stream, stream.Send(NewBeginOfPiece(req.TaskId, req.PeerId))
}

// ReportPeerResult reports downloading result for the peer.
func (sc *client) ReportPeerResult(ctx context.Context, req *schedulerv1.PeerResult, opts ...grpc.CallOption) error {

	ctx = context.WithValue(ctx, consistent.ConsistentHashKey, req.TaskId)
	if _, err := sc.SchedulerClient.ReportPeerResult(ctx, req, opts...); err != nil {
		return err
	}

	return nil
}

// LeaveTask makes the peer leaving from task.
func (sc *client) LeaveTask(ctx context.Context, req *schedulerv1.PeerTarget, opts ...grpc.CallOption) error {

	ctx = context.WithValue(ctx, consistent.ConsistentHashKey, req.TaskId)
	if _, err := sc.SchedulerClient.LeaveTask(ctx, req, opts...); err != nil {
		return err
	}

	return nil
}

// Checks if any peer has the given task.
func (sc *client) StatTask(ctx context.Context, req *schedulerv1.StatTaskRequest, opts ...grpc.CallOption) (*schedulerv1.Task, error) {

	ctx = context.WithValue(ctx, consistent.ConsistentHashKey, req.TaskId)
	resp, err := sc.SchedulerClient.StatTask(ctx, req, opts...)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

// A peer announces that it has the announced task to other peers.
func (sc *client) AnnounceTask(ctx context.Context, req *schedulerv1.AnnounceTaskRequest, opts ...grpc.CallOption) error {

	ctx = context.WithValue(ctx, consistent.ConsistentHashKey, req.TaskId)
	if _, err := sc.SchedulerClient.AnnounceTask(ctx, req, opts...); err != nil {
		return err
	}

	return nil
}
