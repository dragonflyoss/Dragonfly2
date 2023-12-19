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
	"google.golang.org/grpc/balancer"

	dfdaemonv2 "d7y.io/api/v2/pkg/apis/dfdaemon/v2"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	pkgbalancer "d7y.io/dragonfly/v2/pkg/balancer"
	"d7y.io/dragonfly/v2/pkg/resolver"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/scheduler/config"
)

// GetV2 returns v2 version of the dfdaemon client.
func GetV2(ctx context.Context, dynconfig config.DynconfigInterface, opts ...grpc.DialOption) (V2, error) {
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
		DfdaemonUploadClient:           dfdaemonv2.NewDfdaemonUploadClient(conn),
		ClientConn:                     conn,
		ConsistentHashingPickerBuilder: pickerBuilder,
	}, nil
}

// V2 is the interface for v2 version of the grpc client.
type V2 interface {
	// SyncPieces syncs pieces from the other peers.
	SyncPieces(context.Context, *dfdaemonv2.SyncPiecesRequest, ...grpc.CallOption) (dfdaemonv2.DfdaemonUpload_SyncPiecesClient, error)

	// DownloadPiece downloads piece from the other peer.
	DownloadPiece(context.Context, *dfdaemonv2.DownloadPieceRequest, ...grpc.CallOption) (*dfdaemonv2.DownloadPieceResponse, error)

	// TriggerDownloadTask triggers download task from the other peer.
	TriggerDownloadTask(context.Context, *dfdaemonv2.TriggerDownloadTaskRequest, ...grpc.CallOption) error

	// Close tears down the ClientConn and all underlying connections.
	Close() error
}

// v2 provides v2 version of the dfdaemon grpc function.
type v2 struct {
	dfdaemonv2.DfdaemonUploadClient
	*grpc.ClientConn
	*pkgbalancer.ConsistentHashingPickerBuilder
}

// SyncPieces syncs pieces from the other peers.
func (v *v2) SyncPieces(ctx context.Context, req *dfdaemonv2.SyncPiecesRequest, opts ...grpc.CallOption) (dfdaemonv2.DfdaemonUpload_SyncPiecesClient, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.DfdaemonUploadClient.SyncPieces(
		ctx,
		req,
		opts...,
	)
}

// DownloadPiece downloads piece from the other peer.
func (v *v2) DownloadPiece(ctx context.Context, req *dfdaemonv2.DownloadPieceRequest, opts ...grpc.CallOption) (*dfdaemonv2.DownloadPieceResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.DfdaemonUploadClient.DownloadPiece(
		ctx,
		req,
		opts...,
	)
}

// TriggerDownloadTask triggers download task from the other peer.
func (v *v2) TriggerDownloadTask(ctx context.Context, req *dfdaemonv2.TriggerDownloadTaskRequest, opts ...grpc.CallOption) error {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	_, err := v.DfdaemonUploadClient.TriggerDownloadTask(
		ctx,
		req,
		opts...,
	)
	return err
}
