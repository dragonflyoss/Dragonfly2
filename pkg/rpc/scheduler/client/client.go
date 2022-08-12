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
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	commonv1 "d7y.io/api/pkg/apis/common/v1"
	schedulerv1 "d7y.io/api/pkg/apis/scheduler/v1"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/balancer"
	"d7y.io/dragonfly/v2/pkg/resolver"
	"d7y.io/dragonfly/v2/pkg/rpc/common"
)

const (
	// maxRetries is maximum number of retries.
	maxRetries = 3

	// backoffWaitBetween is waiting for a fixed period of
	// time between calls in backoff linear.
	backoffWaitBetween = 500 * time.Millisecond

	// perRetryTimeout is GRPC timeout per call (including initial call) on this call.
	perRetryTimeout = 3 * time.Second
)

// defaultDialOptions is default dial options of manager client.
var defaultDialOptions = []grpc.DialOption{
	grpc.WithDefaultServiceConfig(balancer.BalancerServiceConfig),
	grpc.WithTransportCredentials(insecure.NewCredentials()),
	grpc.WithUnaryInterceptor(grpc_middleware.ChainUnaryClient(
		grpc_prometheus.UnaryClientInterceptor,
		grpc_zap.UnaryClientInterceptor(logger.GrpcLogger.Desugar()),
		grpc_retry.UnaryClientInterceptor(
			grpc_retry.WithPerRetryTimeout(perRetryTimeout),
			grpc_retry.WithMax(maxRetries),
			grpc_retry.WithBackoff(grpc_retry.BackoffLinear(backoffWaitBetween)),
		),
	)),
	grpc.WithStreamInterceptor(grpc_middleware.ChainStreamClient(
		grpc_prometheus.StreamClientInterceptor,
		grpc_zap.StreamClientInterceptor(logger.GrpcLogger.Desugar()),
	)),
}

// GetClient get scheduler clients using resolver and balancer,
func GetClient(options ...grpc.DialOption) (Client, error) {
	conn, err := grpc.Dial(
		resolver.SchedulerVirtualTarget,
		append(defaultDialOptions, options...)...,
	)
	if err != nil {
		return nil, err
	}

	return &client{
		conn,
		schedulerv1.NewSchedulerClient(conn),
	}, nil
}

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
func (c *client) RegisterPeerTask(ctx context.Context, req *schedulerv1.PeerTaskRequest, options ...grpc.CallOption) (*schedulerv1.RegisterResult, error) {
	return c.SchedulerClient.RegisterPeerTask(
		context.WithValue(ctx, balancer.ContextKey, req.TaskId),
		req,
		options...,
	)
}

// ReportPieceResult reports piece results and receives peer packets.
func (c *client) ReportPieceResult(ctx context.Context, req *schedulerv1.PeerTaskRequest, options ...grpc.CallOption) (schedulerv1.Scheduler_ReportPieceResultClient, error) {
	stream, err := c.SchedulerClient.ReportPieceResult(
		context.WithValue(ctx, balancer.ContextKey, req.TaskId),
		options...,
	)
	if err != nil {
		return nil, err
	}

	return stream, stream.Send(NewBeginOfPiece(req.TaskId, req.PeerId))
}

// ReportPeerResult reports downloading result for the peer.
func (c *client) ReportPeerResult(ctx context.Context, req *schedulerv1.PeerResult, options ...grpc.CallOption) error {
	if _, err := c.SchedulerClient.ReportPeerResult(
		context.WithValue(ctx, balancer.ContextKey, req.TaskId),
		req,
		options...,
	); err != nil {
		return err
	}

	return nil
}

// LeaveTask makes the peer leaving from task.
func (c *client) LeaveTask(ctx context.Context, req *schedulerv1.PeerTarget, options ...grpc.CallOption) error {
	if _, err := c.SchedulerClient.LeaveTask(
		context.WithValue(ctx, balancer.ContextKey, req.TaskId),
		req,
		options...,
	); err != nil {
		return err
	}

	return nil
}

// Checks if any peer has the given task.
func (c *client) StatTask(ctx context.Context, req *schedulerv1.StatTaskRequest, options ...grpc.CallOption) (*schedulerv1.Task, error) {
	return c.SchedulerClient.StatTask(
		context.WithValue(ctx, balancer.ContextKey, req.TaskId),
		req,
		options...,
	)
}

// A peer announces that it has the announced task to other peers.
func (c *client) AnnounceTask(ctx context.Context, req *schedulerv1.AnnounceTaskRequest, options ...grpc.CallOption) error {
	if _, err := c.SchedulerClient.AnnounceTask(
		context.WithValue(ctx, balancer.ContextKey, req.TaskId),
		req,
		options...,
	); err != nil {
		return err
	}

	return nil
}
