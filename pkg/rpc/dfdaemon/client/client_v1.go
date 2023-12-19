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

//go:generate mockgen -destination mocks/client_v1_mock.go -source client_v1.go -package mocks

package client

import (
	"context"

	"github.com/google/uuid"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"

	commonv1 "d7y.io/api/v2/pkg/apis/common/v1"
	dfdaemonv1 "d7y.io/api/v2/pkg/apis/dfdaemon/v1"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc"
)

// GetV1 returns v1 version of the dfdaemon client.
func GetV1(ctx context.Context, target string, opts ...grpc.DialOption) (V1, error) {
	if rpc.IsVsock(target) {
		opts = append(opts, grpc.WithContextDialer(rpc.VsockDialer))
	}

	conn, err := grpc.DialContext(
		ctx,
		target,
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

	return &v1{
		DaemonClient: dfdaemonv1.NewDaemonClient(conn),
		ClientConn:   conn,
	}, nil
}

// GetInsecureV1 returns v1 version of the dfdaemon client.
// FIXME use GetV1 and insecure.NewCredentials instead of this function
func GetInsecureV1(ctx context.Context, target string, opts ...grpc.DialOption) (V1, error) {
	return GetV1(ctx, target, append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))...)
}

// V1 is the interface for v1 version of the grpc client.
type V1 interface {
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

// v1 provides v1 version of the dfdaemon grpc function.
type v1 struct {
	dfdaemonv1.DaemonClient
	*grpc.ClientConn
}

// Trigger client to download file.
func (v *v1) Download(ctx context.Context, req *dfdaemonv1.DownRequest, opts ...grpc.CallOption) (dfdaemonv1.Daemon_DownloadClient, error) {
	req.Uuid = uuid.New().String()
	return v.DaemonClient.Download(ctx, req, opts...)
}

// Get piece tasks from other peers.
func (v *v1) GetPieceTasks(ctx context.Context, req *commonv1.PieceTaskRequest, opts ...grpc.CallOption) (*commonv1.PiecePacket, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.DaemonClient.GetPieceTasks(ctx, req, opts...)
}

// Sync piece tasks with other peers.
func (v *v1) SyncPieceTasks(ctx context.Context, req *commonv1.PieceTaskRequest, opts ...grpc.CallOption) (dfdaemonv1.Daemon_SyncPieceTasksClient, error) {
	stream, err := v.DaemonClient.SyncPieceTasks(ctx, opts...)
	if err != nil {
		return nil, err
	}

	return stream, stream.Send(req)
}

// Check if given task exists in P2P cache system.
func (v *v1) StatTask(ctx context.Context, req *dfdaemonv1.StatTaskRequest, opts ...grpc.CallOption) error {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	_, err := v.DaemonClient.StatTask(ctx, req, opts...)
	return err
}

// Import the given file into P2P cache system.
func (v *v1) ImportTask(ctx context.Context, req *dfdaemonv1.ImportTaskRequest, opts ...grpc.CallOption) error {
	_, err := v.DaemonClient.ImportTask(ctx, req, opts...)
	return err
}

// Export or download file from P2P cache system.
func (v *v1) ExportTask(ctx context.Context, req *dfdaemonv1.ExportTaskRequest, opts ...grpc.CallOption) error {
	_, err := v.DaemonClient.ExportTask(ctx, req, opts...)
	return err
}

// Delete file from P2P cache system.
func (v *v1) DeleteTask(ctx context.Context, req *dfdaemonv1.DeleteTaskRequest, opts ...grpc.CallOption) error {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	_, err := v.DaemonClient.DeleteTask(ctx, req, opts...)
	return err
}

// Check daemon health.
func (v *v1) CheckHealth(ctx context.Context, opts ...grpc.CallOption) error {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	_, err := v.DaemonClient.CheckHealth(ctx, new(emptypb.Empty), opts...)
	return err
}
