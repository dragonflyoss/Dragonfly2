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

	cdnsystemv1 "d7y.io/api/pkg/apis/cdnsystem/v1"
	commonv1 "d7y.io/api/pkg/apis/common/v1"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/balancer"
	"d7y.io/dragonfly/v2/pkg/dfnet"
	"d7y.io/dragonfly/v2/pkg/resolver"
)

const (
	backoffBaseDelay  = 1 * time.Second
	backoffMultiplier = 1.2
	backoffJitter     = 0.2
	backoffMaxDelay   = 120 * time.Second
	minConnectTime    = 3 * time.Second //fast fail, leave time to try other scheduler
)

var defaultDialOptions = []grpc.DialOption{
	grpc.WithDefaultServiceConfig(balancer.BalancerServiceConfig),
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
	)),
}

func GetClientByAddr(netAddr dfnet.NetAddr, options ...grpc.DialOption) (Client, error) {
	conn, err := grpc.Dial(
		netAddr.Addr,
		append(defaultDialOptions, options...)...,
	)
	if err != nil {
		return nil, err
	}

	return &client{
		conn,
		cdnsystemv1.NewSeederClient(conn),
	}, nil
}

func GetClient(options ...grpc.DialOption) (Client, error) {
	conn, err := grpc.Dial(
		resolver.SeedPeerVirtualTarget,
		append(defaultDialOptions, options...)...,
	)
	if err != nil {
		return nil, err
	}

	return &client{
		conn,
		cdnsystemv1.NewSeederClient(conn),
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
}

// client provides seed peer grpc function.
type client struct {
	*grpc.ClientConn
	cdnsystemv1.SeederClient
}

// ObtainSeeds triggers the seed peer to download task back-to-source..
func (c *client) ObtainSeeds(ctx context.Context, req *cdnsystemv1.SeedRequest, options ...grpc.CallOption) (cdnsystemv1.Seeder_ObtainSeedsClient, error) {
	return c.SeederClient.ObtainSeeds(
		context.WithValue(ctx, balancer.ContextKey, req.TaskId),
		req,
		options...,
	)
}

// GetPieceTasks gets detail information of task.
func (c *client) GetPieceTasks(ctx context.Context, req *commonv1.PieceTaskRequest, options ...grpc.CallOption) (*commonv1.PiecePacket, error) {
	return c.SeederClient.GetPieceTasks(
		context.WithValue(ctx, balancer.ContextKey, req.TaskId),
		req,
		options...,
	)
}

// SyncPieceTasks syncs detail information of task.
func (c *client) SyncPieceTasks(ctx context.Context, req *commonv1.PieceTaskRequest, options ...grpc.CallOption) (cdnsystemv1.Seeder_SyncPieceTasksClient, error) {
	stream, err := c.SeederClient.SyncPieceTasks(
		context.WithValue(ctx, balancer.ContextKey, req.TaskId),
		options...,
	)
	if err != nil {
		return nil, err
	}

	return stream, stream.Send(req)
}
