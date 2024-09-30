/*
 *     Copyright 2023 The Dragonfly Authors
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

//go:generate mockgen -destination mocks/client_v2_mock.go -source client_v2.go -package mocks

package client

import (
	"context"
	"math"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer"
	"google.golang.org/protobuf/types/known/emptypb"

	commonv2 "d7y.io/api/v2/pkg/apis/common/v2"
	schedulerv2 "d7y.io/api/v2/pkg/apis/scheduler/v2"

	"d7y.io/dragonfly/v2/client/config"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	pkgbalancer "d7y.io/dragonfly/v2/pkg/balancer"
	"d7y.io/dragonfly/v2/pkg/resolver"
	"d7y.io/dragonfly/v2/pkg/rpc"
)

// GetV2 returns v2 version of the scheduler client.
func GetV2(ctx context.Context, dynconfig config.Dynconfig, opts ...grpc.DialOption) (V2, error) {
	// Register resolver and balancer.
	resolver.RegisterScheduler(dynconfig)
	builder, pickerBuilder := pkgbalancer.NewConsistentHashingBuilder()
	balancer.Register(builder)

	conn, err := grpc.DialContext(
		ctx,
		resolver.SchedulerVirtualTarget,
		append([]grpc.DialOption{
			grpc.WithIdleTimeout(0),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(math.MaxInt32),
				grpc.MaxCallSendMsgSize(math.MaxInt32),
			),
			grpc.WithDefaultServiceConfig(pkgbalancer.BalancerServiceConfig),
			grpc.WithUnaryInterceptor(grpc_middleware.ChainUnaryClient(
				grpc_prometheus.UnaryClientInterceptor,
				grpc_zap.UnaryClientInterceptor(logger.GrpcLogger.Desugar()),
				grpc_retry.UnaryClientInterceptor(
					grpc_retry.WithMax(maxRetries),
					grpc_retry.WithBackoff(grpc_retry.BackoffLinear(backoffWaitBetween)),
				),
				rpc.RefresherUnaryClientInterceptor(dynconfig),
			)),
			grpc.WithStreamInterceptor(grpc_middleware.ChainStreamClient(
				grpc_prometheus.StreamClientInterceptor,
				grpc_zap.StreamClientInterceptor(logger.GrpcLogger.Desugar()),
				rpc.RefresherStreamClientInterceptor(dynconfig),
			)),
		}, opts...)...,
	)
	if err != nil {
		return nil, err
	}

	return &v2{
		SchedulerClient:                schedulerv2.NewSchedulerClient(conn),
		ClientConn:                     conn,
		Dynconfig:                      dynconfig,
		dialOptions:                    opts,
		ConsistentHashingPickerBuilder: pickerBuilder,
	}, nil
}

// GetV2ByAddr returns v2 version of the scheduler client by address.
func GetV2ByAddr(ctx context.Context, target string, opts ...grpc.DialOption) (V2, error) {
	conn, err := grpc.DialContext(
		ctx,
		target,
		append([]grpc.DialOption{
			grpc.WithIdleTimeout(0),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(math.MaxInt32),
				grpc.MaxCallSendMsgSize(math.MaxInt32),
			),
			grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
			grpc.WithDefaultServiceConfig(pkgbalancer.BalancerServiceConfig),
			grpc.WithUnaryInterceptor(grpc_middleware.ChainUnaryClient(
				grpc_prometheus.UnaryClientInterceptor,
				grpc_zap.UnaryClientInterceptor(logger.GrpcLogger.Desugar()),
				grpc_retry.UnaryClientInterceptor(
					grpc_retry.WithMax(maxRetries),
					grpc_retry.WithBackoff(grpc_retry.BackoffLinear(backoffWaitBetween)),
				),
			)),
			grpc.WithStreamInterceptor(grpc_middleware.ChainStreamClient(
				grpc_prometheus.StreamClientInterceptor,
				grpc_zap.StreamClientInterceptor(logger.GrpcLogger.Desugar()),
			)),
		}, opts...)...,
	)
	if err != nil {
		return nil, err
	}

	return &v2{
		SchedulerClient: schedulerv2.NewSchedulerClient(conn),
		ClientConn:      conn,
		dialOptions:     opts,
	}, nil
}

// V2 is the interface for v1 version of the grpc client.
type V2 interface {
	// AnnouncePeer announces peer to scheduler.
	AnnouncePeer(context.Context, string, ...grpc.CallOption) (schedulerv2.Scheduler_AnnouncePeerClient, error)

	// Checks information of peer.
	StatPeer(context.Context, *schedulerv2.StatPeerRequest, ...grpc.CallOption) (*commonv2.Peer, error)

	// DeletePeer releases peer in scheduler.
	DeletePeer(context.Context, *schedulerv2.DeletePeerRequest, ...grpc.CallOption) error

	// Checks information of task.
	StatTask(context.Context, *schedulerv2.StatTaskRequest, ...grpc.CallOption) (*commonv2.Task, error)

	// DeleteTask releases task in scheduler.
	DeleteTask(context.Context, *schedulerv2.DeleteTaskRequest, ...grpc.CallOption) error

	// AnnounceHost announces host to scheduler.
	AnnounceHost(context.Context, *schedulerv2.AnnounceHostRequest, ...grpc.CallOption) error

	// ListHosts lists hosts in scheduler.
	ListHosts(ctx context.Context, taskID string, opts ...grpc.CallOption) (*schedulerv2.ListHostsResponse, error)

	// DeleteHost releases host in scheduler.
	DeleteHost(context.Context, *schedulerv2.DeleteHostRequest, ...grpc.CallOption) error

	// Close tears down the ClientConn and all underlying connections.
	Close() error
}

// v2 provides v2 version of the scheduler grpc function.
type v2 struct {
	schedulerv2.SchedulerClient
	*grpc.ClientConn
	config.Dynconfig
	dialOptions []grpc.DialOption
	*pkgbalancer.ConsistentHashingPickerBuilder
}

// AnnouncePeer announces peer to scheduler.
func (v *v2) AnnouncePeer(ctx context.Context, taskID string, opts ...grpc.CallOption) (schedulerv2.Scheduler_AnnouncePeerClient, error) {
	return v.SchedulerClient.AnnouncePeer(
		context.WithValue(ctx, pkgbalancer.ContextKey, taskID),
		opts...,
	)
}

// Checks information of peer.
func (v *v2) StatPeer(ctx context.Context, req *schedulerv2.StatPeerRequest, opts ...grpc.CallOption) (*commonv2.Peer, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.SchedulerClient.StatPeer(
		context.WithValue(ctx, pkgbalancer.ContextKey, req.TaskId),
		req,
		opts...,
	)
}

// DeletePeer releases peer in scheduler.
func (v *v2) DeletePeer(ctx context.Context, req *schedulerv2.DeletePeerRequest, opts ...grpc.CallOption) error {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	_, err := v.SchedulerClient.DeletePeer(
		context.WithValue(ctx, pkgbalancer.ContextKey, req.TaskId),
		req,
		opts...,
	)

	return err
}

// Checks information of task.
func (v *v2) StatTask(ctx context.Context, req *schedulerv2.StatTaskRequest, opts ...grpc.CallOption) (*commonv2.Task, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.SchedulerClient.StatTask(
		context.WithValue(ctx, pkgbalancer.ContextKey, req.TaskId),
		req,
		opts...,
	)
}

// DeleteTask releases task in scheduler.
func (v *v2) DeleteTask(ctx context.Context, req *schedulerv2.DeleteTaskRequest, opts ...grpc.CallOption) error {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	_, err := v.SchedulerClient.DeleteTask(
		context.WithValue(ctx, pkgbalancer.ContextKey, req.TaskId),
		req,
		opts...,
	)

	return err
}

// AnnounceHost announces host to scheduler.
func (v *v2) AnnounceHost(ctx context.Context, req *schedulerv2.AnnounceHostRequest, opts ...grpc.CallOption) error {
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

// ListHosts lists host in all schedulers.
func (v *v2) ListHosts(ctx context.Context, taskID string, opts ...grpc.CallOption) (*schedulerv2.ListHostsResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.SchedulerClient.ListHosts(
		context.WithValue(ctx, pkgbalancer.ContextKey, taskID),
		new(emptypb.Empty),
		opts...,
	)
}

// DeleteHost releases host in all schedulers.
func (v *v2) DeleteHost(ctx context.Context, req *schedulerv2.DeleteHostRequest, opts ...grpc.CallOption) error {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	circle, err := v.GetCircle()
	if err != nil {
		return err
	}
	logger.Infof("delete host circle is %#v", circle)

	eg, _ := errgroup.WithContext(ctx)
	for _, virtualTaskID := range circle {
		virtualTaskID := virtualTaskID
		eg.Go(func() error {
			if _, err := v.SchedulerClient.DeleteHost(
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
