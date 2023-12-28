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

//go:generate mockgen -destination mocks/client_v1_mock.go -source client_v1.go -package mocks

package client

import (
	"context"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer"

	commonv1 "d7y.io/api/v2/pkg/apis/common/v1"
	schedulerv1 "d7y.io/api/v2/pkg/apis/scheduler/v1"

	"d7y.io/dragonfly/v2/client/config"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	pkgbalancer "d7y.io/dragonfly/v2/pkg/balancer"
	"d7y.io/dragonfly/v2/pkg/resolver"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/common"
)

// GetV1 returns v1 version of the scheduler client.
func GetV1(ctx context.Context, dynconfig config.Dynconfig, opts ...grpc.DialOption) (V1, error) {
	// Register resolver and balancer.
	resolver.RegisterScheduler(dynconfig)
	builder, pickerBuilder := pkgbalancer.NewConsistentHashingBuilder()
	balancer.Register(builder)

	conn, err := grpc.DialContext(
		ctx,
		resolver.SchedulerVirtualTarget,
		append([]grpc.DialOption{
			grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
			grpc.WithDefaultServiceConfig(pkgbalancer.BalancerServiceConfig),
			grpc.WithUnaryInterceptor(grpc_middleware.ChainUnaryClient(
				rpc.ConvertErrorUnaryClientInterceptor,
				grpc_prometheus.UnaryClientInterceptor,
				grpc_zap.UnaryClientInterceptor(logger.GrpcLogger.Desugar()),
				grpc_retry.UnaryClientInterceptor(
					grpc_retry.WithMax(maxRetries),
					grpc_retry.WithBackoff(grpc_retry.BackoffLinear(backoffWaitBetween)),
				),
				rpc.RefresherUnaryClientInterceptor(dynconfig),
			)),
			grpc.WithStreamInterceptor(grpc_middleware.ChainStreamClient(
				rpc.ConvertErrorStreamClientInterceptor,
				grpc_prometheus.StreamClientInterceptor,
				grpc_zap.StreamClientInterceptor(logger.GrpcLogger.Desugar()),
				rpc.RefresherStreamClientInterceptor(dynconfig),
			)),
		}, opts...)...,
	)
	if err != nil {
		return nil, err
	}

	return &v1{
		SchedulerClient:                schedulerv1.NewSchedulerClient(conn),
		ClientConn:                     conn,
		Dynconfig:                      dynconfig,
		dialOptions:                    opts,
		ConsistentHashingPickerBuilder: pickerBuilder,
	}, nil
}

// GetV1ByAddr returns v2 version of the scheduler client by address.
func GetV1ByAddr(ctx context.Context, target string, opts ...grpc.DialOption) (V1, error) {
	conn, err := grpc.DialContext(
		ctx,
		target,
		append([]grpc.DialOption{
			grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
			grpc.WithDefaultServiceConfig(pkgbalancer.BalancerServiceConfig),
			grpc.WithUnaryInterceptor(grpc_middleware.ChainUnaryClient(
				rpc.ConvertErrorUnaryClientInterceptor,
				grpc_prometheus.UnaryClientInterceptor,
				grpc_zap.UnaryClientInterceptor(logger.GrpcLogger.Desugar()),
				grpc_retry.UnaryClientInterceptor(
					grpc_retry.WithMax(maxRetries),
					grpc_retry.WithBackoff(grpc_retry.BackoffLinear(backoffWaitBetween)),
				),
			)),
			grpc.WithStreamInterceptor(grpc_middleware.ChainStreamClient(
				rpc.ConvertErrorStreamClientInterceptor,
				grpc_prometheus.StreamClientInterceptor,
				grpc_zap.StreamClientInterceptor(logger.GrpcLogger.Desugar()),
			)),
		}, opts...)...,
	)
	if err != nil {
		return nil, err
	}

	return &v1{
		SchedulerClient: schedulerv1.NewSchedulerClient(conn),
		ClientConn:      conn,
		dialOptions:     opts,
	}, nil
}

// V1 is the interface for v1 version of the grpc client.
type V1 interface {
	// RegisterPeerTask registers a peer into task.
	RegisterPeerTask(context.Context, *schedulerv1.PeerTaskRequest, ...grpc.CallOption) (*schedulerv1.RegisterResult, error)

	// ReportPieceResult reports piece results and receives peer packets.
	ReportPieceResult(context.Context, *schedulerv1.PeerTaskRequest, ...grpc.CallOption) (schedulerv1.Scheduler_ReportPieceResultClient, error)

	// ReportPeerResult reports downloading result for the peer.
	ReportPeerResult(context.Context, *schedulerv1.PeerResult, ...grpc.CallOption) error

	// A peer announces that it has the announced task to other peers.
	AnnounceTask(context.Context, *schedulerv1.AnnounceTaskRequest, ...grpc.CallOption) error

	// Checks if any peer has the given task.
	StatTask(context.Context, *schedulerv1.StatTaskRequest, ...grpc.CallOption) (*schedulerv1.Task, error)

	// LeaveTask releases peer in scheduler.
	LeaveTask(context.Context, *schedulerv1.PeerTarget, ...grpc.CallOption) error

	// AnnounceHost announces host to scheduler.
	AnnounceHost(context.Context, *schedulerv1.AnnounceHostRequest, ...grpc.CallOption) error

	// LeaveHost releases host in scheduler.
	LeaveHost(context.Context, *schedulerv1.LeaveHostRequest, ...grpc.CallOption) error

	// SyncProbes sync probes of the host.
	SyncProbes(context.Context, *schedulerv1.SyncProbesRequest, ...grpc.CallOption) (schedulerv1.Scheduler_SyncProbesClient, error)

	// Close tears down the ClientConn and all underlying connections.
	Close() error
}

// v1 provides v1 version of the scheduler grpc function.
type v1 struct {
	schedulerv1.SchedulerClient
	*grpc.ClientConn
	config.Dynconfig
	dialOptions []grpc.DialOption
	*pkgbalancer.ConsistentHashingPickerBuilder
}

// RegisterPeerTask registers a peer into task.
func (v *v1) RegisterPeerTask(ctx context.Context, req *schedulerv1.PeerTaskRequest, opts ...grpc.CallOption) (*schedulerv1.RegisterResult, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.SchedulerClient.RegisterPeerTask(
		context.WithValue(ctx, pkgbalancer.ContextKey, req.TaskId),
		req,
		opts...,
	)
}

// ReportPieceResult reports piece results and receives peer packets.
func (v *v1) ReportPieceResult(ctx context.Context, req *schedulerv1.PeerTaskRequest, opts ...grpc.CallOption) (schedulerv1.Scheduler_ReportPieceResultClient, error) {
	stream, err := v.SchedulerClient.ReportPieceResult(
		context.WithValue(ctx, pkgbalancer.ContextKey, req.TaskId),
		opts...,
	)
	if err != nil {
		return nil, err
	}

	// Send begin of piece.
	return stream, stream.Send(&schedulerv1.PieceResult{
		TaskId: req.TaskId,
		SrcPid: req.PeerId,
		PieceInfo: &commonv1.PieceInfo{
			PieceNum: common.BeginOfPiece,
		},
	})
}

// ReportPeerResult reports downloading result for the peer.
func (v *v1) ReportPeerResult(ctx context.Context, req *schedulerv1.PeerResult, opts ...grpc.CallOption) error {
	_, err := v.SchedulerClient.ReportPeerResult(
		context.WithValue(ctx, pkgbalancer.ContextKey, req.TaskId),
		req,
		opts...,
	)

	return err
}

// A peer announces that it has the announced task to other peers.
func (v *v1) AnnounceTask(ctx context.Context, req *schedulerv1.AnnounceTaskRequest, opts ...grpc.CallOption) error {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	_, err := v.SchedulerClient.AnnounceTask(
		context.WithValue(ctx, pkgbalancer.ContextKey, req.TaskId),
		req,
		opts...,
	)

	return err
}

// Checks if any peer has the given task.
func (v *v1) StatTask(ctx context.Context, req *schedulerv1.StatTaskRequest, opts ...grpc.CallOption) (*schedulerv1.Task, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.SchedulerClient.StatTask(
		context.WithValue(ctx, pkgbalancer.ContextKey, req.TaskId),
		req,
		opts...,
	)
}

// LeaveTask releases peer in scheduler.
func (v *v1) LeaveTask(ctx context.Context, req *schedulerv1.PeerTarget, opts ...grpc.CallOption) error {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	_, err := v.SchedulerClient.LeaveTask(
		context.WithValue(ctx, pkgbalancer.ContextKey, req.TaskId),
		req,
		opts...,
	)

	return err
}

// AnnounceHost announces host to scheduler.
func (v *v1) AnnounceHost(ctx context.Context, req *schedulerv1.AnnounceHostRequest, opts ...grpc.CallOption) error {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	circle, err := v.GetCircle()
	if err != nil {
		return err
	}

	eg, _ := errgroup.WithContext(ctx)
	for _, virtualTaskID := range circle {
		virtualTaskID := virtualTaskID
		eg.Go(func() error {
			if _, err := v.SchedulerClient.AnnounceHost(
				context.WithValue(ctx, pkgbalancer.ContextKey, virtualTaskID),
				req,
				opts...,
			); err != nil {
				return err
			}

			return nil
		})
	}

	return eg.Wait()
}

// LeaveHost releases host in all schedulers.
func (v *v1) LeaveHost(ctx context.Context, req *schedulerv1.LeaveHostRequest, opts ...grpc.CallOption) error {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	circle, err := v.GetCircle()
	if err != nil {
		return err
	}
	logger.Infof("leave host circle is %#v", circle)

	eg, _ := errgroup.WithContext(ctx)
	for _, virtualTaskID := range circle {
		virtualTaskID := virtualTaskID
		eg.Go(func() error {
			if _, err := v.SchedulerClient.LeaveHost(
				context.WithValue(ctx, pkgbalancer.ContextKey, virtualTaskID),
				req,
				opts...,
			); err != nil {
				return err
			}

			return nil
		})
	}

	return eg.Wait()
}

// SyncProbes sync probes of the host.
func (v *v1) SyncProbes(ctx context.Context, req *schedulerv1.SyncProbesRequest, opts ...grpc.CallOption) (schedulerv1.Scheduler_SyncProbesClient, error) {
	stream, err := v.SchedulerClient.SyncProbes(
		context.WithValue(ctx, pkgbalancer.ContextKey, req.Host.Id),
		opts...,
	)
	if err != nil {
		return nil, err
	}

	// Send begin of piece.
	return stream, stream.Send(req)
}
