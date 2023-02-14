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

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

	commonv2 "d7y.io/api/pkg/apis/common/v2"
	dfdaemonv2 "d7y.io/api/pkg/apis/dfdaemon/v2"

	logger "d7y.io/dragonfly/v2/internal/dflog"
)

// GetV2 returns v2 version of the dfdaemon client.
func GetV2(ctx context.Context, target string, opts ...grpc.DialOption) (V2, error) {
	conn, err := grpc.DialContext(
		ctx,
		target,
		append([]grpc.DialOption{
			grpc.WithUnaryInterceptor(grpc_middleware.ChainUnaryClient(
				otelgrpc.UnaryClientInterceptor(),
				grpc_prometheus.UnaryClientInterceptor,
				grpc_zap.UnaryClientInterceptor(logger.GrpcLogger.Desugar()),
				grpc_retry.UnaryClientInterceptor(
					grpc_retry.WithMax(maxRetries),
					grpc_retry.WithBackoff(grpc_retry.BackoffLinear(backoffWaitBetween)),
				),
			)),
			grpc.WithStreamInterceptor(grpc_middleware.ChainStreamClient(
				otelgrpc.StreamClientInterceptor(),
				grpc_prometheus.StreamClientInterceptor,
				grpc_zap.StreamClientInterceptor(logger.GrpcLogger.Desugar()),
			)),
		}, opts...)...,
	)
	if err != nil {
		return nil, err
	}

	return &v2{
		DfdaemonClient: dfdaemonv2.NewDfdaemonClient(conn),
		ClientConn:     conn,
	}, nil
}

// V2 is the interface for v2 version of the grpc client.
type V2 interface {
	// SyncPieces syncs pieces from the other peers.
	SyncPieces(context.Context, ...grpc.CallOption) (dfdaemonv2.Dfdaemon_SyncPiecesClient, error)

	// DownloadTask downloads task back-to-source.
	DownloadTask(context.Context, *dfdaemonv2.DownloadTaskRequest, ...grpc.CallOption) error

	// UploadTask uploads task to p2p network.
	UploadTask(context.Context, *dfdaemonv2.UploadTaskRequest, ...grpc.CallOption) error

	// StatTask stats task information.
	StatTask(context.Context, *dfdaemonv2.StatTaskRequest, ...grpc.CallOption) (*commonv2.Task, error)

	// DeleteTask deletes task from p2p network.
	DeleteTask(context.Context, *dfdaemonv2.DeleteTaskRequest, ...grpc.CallOption) error

	// Close tears down the ClientConn and all underlying connections.
	Close() error
}

// v2 provides v2 version of the dfdaemon grpc function.
type v2 struct {
	dfdaemonv2.DfdaemonClient
	*grpc.ClientConn
}

// Trigger client to download file.
func (v *v2) SyncPieces(ctx context.Context, opts ...grpc.CallOption) (dfdaemonv2.Dfdaemon_SyncPiecesClient, error) {
	return v.DfdaemonClient.SyncPieces(
		ctx,
		opts...,
	)
}

// DownloadTask downloads task back-to-source.
func (v *v2) DownloadTask(ctx context.Context, req *dfdaemonv2.DownloadTaskRequest, opts ...grpc.CallOption) error {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	_, err := v.DfdaemonClient.DownloadTask(
		ctx,
		req,
		opts...,
	)

	return err
}

// UploadTask uploads task to p2p network.
func (v *v2) UploadTask(ctx context.Context, req *dfdaemonv2.UploadTaskRequest, opts ...grpc.CallOption) error {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	_, err := v.DfdaemonClient.UploadTask(
		ctx,
		req,
		opts...,
	)

	return err
}

// StatTask stats task information.
func (v *v2) StatTask(ctx context.Context, req *dfdaemonv2.StatTaskRequest, opts ...grpc.CallOption) (*commonv2.Task, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.DfdaemonClient.StatTask(
		ctx,
		req,
		opts...,
	)
}

// DeleteTask deletes task from p2p network.
func (v *v2) DeleteTask(ctx context.Context, req *dfdaemonv2.DeleteTaskRequest, opts ...grpc.CallOption) error {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	_, err := v.DfdaemonClient.DeleteTask(
		ctx,
		req,
		opts...,
	)

	return err
}
