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

	"github.com/google/uuid"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"

	commonv1 "d7y.io/api/pkg/apis/common/v1"
	dfdaemonv1 "d7y.io/api/pkg/apis/dfdaemon/v1"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc"
)

const (
	// maxRetries is maximum number of retries.
	maxRetries = 3

	// backoffWaitBetween is waiting for a fixed period of
	// time between calls in backoff linear.
	backoffWaitBetween = 500 * time.Millisecond
)

// GetClient returns dfdaemon client.
func GetClient(ctx context.Context, target string, opts ...grpc.DialOption) (Client, error) {
	if rpc.IsVsock(target) {
		opts = append(opts, grpc.WithContextDialer(rpc.VsockDialer))
	}

	conn, err := grpc.DialContext(
		ctx,
		target,
		append([]grpc.DialOption{
			grpc.WithUnaryInterceptor(grpc_middleware.ChainUnaryClient(
				rpc.ConvertErrorUnaryClientInterceptor,
				otelgrpc.UnaryClientInterceptor(),
				grpc_prometheus.UnaryClientInterceptor,
				grpc_zap.UnaryClientInterceptor(logger.GrpcLogger.Desugar()),
				grpc_retry.UnaryClientInterceptor(
					grpc_retry.WithMax(maxRetries),
					grpc_retry.WithBackoff(grpc_retry.BackoffLinear(backoffWaitBetween)),
				),
			)),
			grpc.WithStreamInterceptor(grpc_middleware.ChainStreamClient(
				rpc.ConvertErrorStreamClientInterceptor,
				otelgrpc.StreamClientInterceptor(),
				grpc_prometheus.StreamClientInterceptor,
				grpc_zap.StreamClientInterceptor(logger.GrpcLogger.Desugar()),
			)),
		}, opts...)...,
	)
	if err != nil {
		return nil, err
	}

	return &client{
		DaemonClient: dfdaemonv1.NewDaemonClient(conn),
		ClientConn:   conn,
	}, nil
}

// GetInsecureClient returns dfdaemon client.
// FIXME use GetClient + insecure.NewCredentials instead of this function
func GetInsecureClient(ctx context.Context, target string, opts ...grpc.DialOption) (Client, error) {
	return GetClient(ctx, target, append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))...)
}

// Client is the interface for grpc client.
type Client interface {
	// Trigger client to download file.
	Download(context.Context, *dfdaemonv1.DownRequest, ...grpc.CallOption) (dfdaemonv1.Daemon_DownloadClient, error)

	// Get piece tasks from other peers.
	GetPieceTasks(context.Context, *commonv1.PieceTaskRequest, ...grpc.CallOption) (*commonv1.PiecePacket, error)

	// Sync piece tasks with other peers.
	SyncPieceTasks(context.Context, *commonv1.PieceTaskRequest, ...grpc.CallOption) (dfdaemonv1.Daemon_SyncPieceTasksClient, error)

	// Check if given task exists in P2P cache system.
	StatTask(context.Context, *dfdaemonv1.StatTaskRequest, ...grpc.CallOption) error

	// Import the given file into P2P cache system.
	ImportTask(context.Context, *dfdaemonv1.ImportTaskRequest, ...grpc.CallOption) error

	// Export or download file from P2P cache system.
	ExportTask(context.Context, *dfdaemonv1.ExportTaskRequest, ...grpc.CallOption) error

	// Delete file from P2P cache system.
	DeleteTask(context.Context, *dfdaemonv1.DeleteTaskRequest, ...grpc.CallOption) error

	// Check daemon health.
	CheckHealth(context.Context, ...grpc.CallOption) error

	// Close tears down the ClientConn and all underlying connections.
	Close() error
}

// client provides dfdaemon grpc function.
type client struct {
	dfdaemonv1.DaemonClient
	*grpc.ClientConn
}

// Trigger client to download file.
func (c *client) Download(ctx context.Context, req *dfdaemonv1.DownRequest, opts ...grpc.CallOption) (dfdaemonv1.Daemon_DownloadClient, error) {
	req.Uuid = uuid.New().String()
	return c.DaemonClient.Download(ctx, req, opts...)
}

// Get piece tasks from other peers.
func (c *client) GetPieceTasks(ctx context.Context, req *commonv1.PieceTaskRequest, opts ...grpc.CallOption) (*commonv1.PiecePacket, error) {
	return c.DaemonClient.GetPieceTasks(ctx, req, opts...)
}

// Sync piece tasks with other peers.
func (c *client) SyncPieceTasks(ctx context.Context, req *commonv1.PieceTaskRequest, opts ...grpc.CallOption) (dfdaemonv1.Daemon_SyncPieceTasksClient, error) {
	stream, err := c.DaemonClient.SyncPieceTasks(ctx, opts...)
	if err != nil {
		return nil, err
	}

	return stream, stream.Send(req)
}

// Check if given task exists in P2P cache system.
func (c *client) StatTask(ctx context.Context, req *dfdaemonv1.StatTaskRequest, opts ...grpc.CallOption) error {
	_, err := c.DaemonClient.StatTask(ctx, req, opts...)
	return err
}

// Import the given file into P2P cache system.
func (c *client) ImportTask(ctx context.Context, req *dfdaemonv1.ImportTaskRequest, opts ...grpc.CallOption) error {
	_, err := c.DaemonClient.ImportTask(ctx, req, opts...)
	return err
}

// Export or download file from P2P cache system.
func (c *client) ExportTask(ctx context.Context, req *dfdaemonv1.ExportTaskRequest, opts ...grpc.CallOption) error {
	_, err := c.DaemonClient.ExportTask(ctx, req, opts...)
	return err
}

// Delete file from P2P cache system.
func (c *client) DeleteTask(ctx context.Context, req *dfdaemonv1.DeleteTaskRequest, opts ...grpc.CallOption) error {
	_, err := c.DaemonClient.DeleteTask(ctx, req, opts...)
	return err
}

// Check daemon health.
func (c *client) CheckHealth(ctx context.Context, opts ...grpc.CallOption) error {
	_, err := c.DaemonClient.CheckHealth(ctx, new(emptypb.Empty), opts...)
	return err
}
