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
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer"

	cdnsystemv1 "d7y.io/api/v2/pkg/apis/cdnsystem/v1"
	commonv1 "d7y.io/api/v2/pkg/apis/common/v1"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	pkgbalancer "d7y.io/dragonfly/v2/pkg/balancer"
	"d7y.io/dragonfly/v2/pkg/dfnet"
	"d7y.io/dragonfly/v2/pkg/resolver"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/scheduler/config"
)

const (
	// contextTimeout is timeout of grpc invoke.
	contextTimeout = 2 * time.Minute

	// maxRetries is maximum number of retries.
	maxRetries = 3

	// backoffWaitBetween is waiting for a fixed period of
	// time between calls in backoff linear.
	backoffWaitBetween = 500 * time.Millisecond
)

func GetClientByAddr(ctx context.Context, netAddr dfnet.NetAddr, opts ...grpc.DialOption) (Client, error) {
	conn, err := grpc.DialContext(
		ctx,
		netAddr.Addr,
		append([]grpc.DialOption{
			grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
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

	return &client{
		SeederClient: cdnsystemv1.NewSeederClient(conn),
		ClientConn:   conn,
	}, nil
}

func GetClient(ctx context.Context, dynconfig config.DynconfigInterface, opts ...grpc.DialOption) (Client, error) {
	// Register resolver and balancer.
	resolver.RegisterSeedPeer(dynconfig)
	builder, pickerBuilder := pkgbalancer.NewConsistentHashingBuilder()
	balancer.Register(builder)

	conn, err := grpc.DialContext(
		ctx,
		resolver.SeedPeerVirtualTarget,
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

	return &client{
		SeederClient:                   cdnsystemv1.NewSeederClient(conn),
		ClientConn:                     conn,
		ConsistentHashingPickerBuilder: pickerBuilder,
	}, nil
}

// Client is the interface for grpc client.
type Client interface {
	// ObtainSeeds triggers the seed peer to download task back-to-source..
	ObtainSeeds(context.Context, *cdnsystemv1.SeedRequest, ...grpc.CallOption) (cdnsystemv1.Seeder_ObtainSeedsClient, error)

	// GetPieceTasks gets detail information of task.
	GetPieceTasks(context.Context, *commonv1.PieceTaskRequest, ...grpc.CallOption) (*commonv1.PiecePacket, error)

	// SyncPieceTasks syncs detail information of task.
	SyncPieceTasks(context.Context, *commonv1.PieceTaskRequest, ...grpc.CallOption) (cdnsystemv1.Seeder_SyncPieceTasksClient, error)

	// Close tears down the ClientConn and all underlying connections.
	Close() error
}

// client provides seed peer grpc function.
type client struct {
	cdnsystemv1.SeederClient
	*grpc.ClientConn
	*pkgbalancer.ConsistentHashingPickerBuilder
}

// ObtainSeeds triggers the seed peer to download task back-to-source..
func (c *client) ObtainSeeds(ctx context.Context, req *cdnsystemv1.SeedRequest, opts ...grpc.CallOption) (cdnsystemv1.Seeder_ObtainSeedsClient, error) {
	return c.SeederClient.ObtainSeeds(
		context.WithValue(ctx, pkgbalancer.ContextKey, req.TaskId),
		req,
		opts...,
	)
}

// GetPieceTasks gets detail information of task.
func (c *client) GetPieceTasks(ctx context.Context, req *commonv1.PieceTaskRequest, opts ...grpc.CallOption) (*commonv1.PiecePacket, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return c.SeederClient.GetPieceTasks(
		context.WithValue(ctx, pkgbalancer.ContextKey, req.TaskId),
		req,
		opts...,
	)
}

// SyncPieceTasks syncs detail information of task.
func (c *client) SyncPieceTasks(ctx context.Context, req *commonv1.PieceTaskRequest, opts ...grpc.CallOption) (cdnsystemv1.Seeder_SyncPieceTasksClient, error) {
	stream, err := c.SeederClient.SyncPieceTasks(
		context.WithValue(ctx, pkgbalancer.ContextKey, req.TaskId),
		opts...,
	)
	if err != nil {
		return nil, err
	}

	return stream, stream.Send(req)
}
