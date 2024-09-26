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
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/credentials/insecure"

	commonv2 "d7y.io/api/v2/pkg/apis/common/v2"
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
		DfdaemonUploadClient:           dfdaemonv2.NewDfdaemonUploadClient(conn),
		ClientConn:                     conn,
		ConsistentHashingPickerBuilder: pickerBuilder,
	}, nil
}

// GetV2ByAddr returns v2 version of the dfdaemon client by address.
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
			grpc.WithUnaryInterceptor(grpc_middleware.ChainUnaryClient(
				grpc_prometheus.UnaryClientInterceptor,
				grpc_zap.UnaryClientInterceptor(logger.GrpcLogger.Desugar()),
				grpc_retry.UnaryClientInterceptor(
					grpc_retry.WithMax(maxRetries),
					grpc_retry.WithBackoff(grpc_retry.BackoffLinear(backoffWaitBetween)),
				),
			)),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
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
		DfdaemonUploadClient: dfdaemonv2.NewDfdaemonUploadClient(conn),
		ClientConn:           conn,
	}, nil
}

// V2 is the interface for v2 version of the grpc client.
type V2 interface {
	// SyncPieces syncs pieces from the other peers.
	SyncPieces(context.Context, *dfdaemonv2.SyncPiecesRequest, ...grpc.CallOption) (dfdaemonv2.DfdaemonUpload_SyncPiecesClient, error)

	// DownloadPiece downloads piece from the other peer.
	DownloadPiece(context.Context, *dfdaemonv2.DownloadPieceRequest, ...grpc.CallOption) (*dfdaemonv2.DownloadPieceResponse, error)

	// DownloadTask downloads task from p2p network.
	DownloadTask(context.Context, string, *dfdaemonv2.DownloadTaskRequest, ...grpc.CallOption) (dfdaemonv2.DfdaemonUpload_DownloadTaskClient, error)

	// StatTask stats task information.
	StatTask(context.Context, *dfdaemonv2.StatTaskRequest, ...grpc.CallOption) (*commonv2.Task, error)

	// DeleteTask deletes task from p2p network.
	DeleteTask(context.Context, *dfdaemonv2.DeleteTaskRequest, ...grpc.CallOption) error

	// DownloadPersistentCacheTask downloads persistent cache task from p2p network.
	DownloadPersistentCacheTask(context.Context, *dfdaemonv2.DownloadPersistentCacheTaskRequest, ...grpc.CallOption) (dfdaemonv2.DfdaemonUpload_DownloadPersistentCacheTaskClient, error)

	// StatPersistentCacheTask stats persistent cache task information.
	StatPersistentCacheTask(context.Context, *dfdaemonv2.StatPersistentCacheTaskRequest, ...grpc.CallOption) (*commonv2.PersistentCacheTask, error)

	// DeletePersistentCacheTask deletes persistent cache task from p2p network.
	DeletePersistentCacheTask(context.Context, *dfdaemonv2.DeletePersistentCacheTaskRequest, ...grpc.CallOption) error

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
		context.WithValue(ctx, pkgbalancer.ContextKey, req.TaskId),
		req,
		opts...,
	)
}

// DownloadPiece downloads piece from the other peer.
func (v *v2) DownloadPiece(ctx context.Context, req *dfdaemonv2.DownloadPieceRequest, opts ...grpc.CallOption) (*dfdaemonv2.DownloadPieceResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.DfdaemonUploadClient.DownloadPiece(
		context.WithValue(ctx, pkgbalancer.ContextKey, req.TaskId),
		req,
		opts...,
	)
}

// DownloadTask downloads task from p2p network.
func (v *v2) DownloadTask(ctx context.Context, taskID string, req *dfdaemonv2.DownloadTaskRequest, opts ...grpc.CallOption) (dfdaemonv2.DfdaemonUpload_DownloadTaskClient, error) {
	return v.DfdaemonUploadClient.DownloadTask(
		context.WithValue(ctx, pkgbalancer.ContextKey, taskID),
		req,
		opts...,
	)
}

// StatTask stats task information.
func (v *v2) StatTask(ctx context.Context, req *dfdaemonv2.StatTaskRequest, opts ...grpc.CallOption) (*commonv2.Task, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.DfdaemonUploadClient.StatTask(ctx, req, opts...)
}

// DeleteTask deletes task from p2p network.
func (v *v2) DeleteTask(ctx context.Context, req *dfdaemonv2.DeleteTaskRequest, opts ...grpc.CallOption) error {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	_, err := v.DfdaemonUploadClient.DeleteTask(ctx, req, opts...)
	return err
}

// DownloadPersistentCacheTask downloads persistent cache task from p2p network.
func (v *v2) DownloadPersistentCacheTask(ctx context.Context, req *dfdaemonv2.DownloadPersistentCacheTaskRequest, opts ...grpc.CallOption) (dfdaemonv2.DfdaemonUpload_DownloadPersistentCacheTaskClient, error) {
	return v.DfdaemonUploadClient.DownloadPersistentCacheTask(ctx, req, opts...)
}

// StatPersistentCacheTask stats persistent cache task information.
func (v *v2) StatPersistentCacheTask(ctx context.Context, req *dfdaemonv2.StatPersistentCacheTaskRequest, opts ...grpc.CallOption) (*commonv2.PersistentCacheTask, error) {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	return v.DfdaemonUploadClient.StatPersistentCacheTask(ctx, req, opts...)
}

// DeletePersistentCacheTask deletes persistent cache task from p2p network.
func (v *v2) DeletePersistentCacheTask(ctx context.Context, req *dfdaemonv2.DeletePersistentCacheTaskRequest, opts ...grpc.CallOption) error {
	ctx, cancel := context.WithTimeout(ctx, contextTimeout)
	defer cancel()

	_, err := v.DfdaemonUploadClient.DeletePersistentCacheTask(ctx, req, opts...)
	return err
}
