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

//go:generate mockgen -destination mocks/server_mock.go -source server.go -package mocks

package server

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/types/known/emptypb"

	commonv1 "d7y.io/api/pkg/apis/common/v1"
	dfdaemonv1 "d7y.io/api/pkg/apis/dfdaemon/v1"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc"
)

// DaemonServer refer to dfdaemonv1.DaemonServer
type DaemonServer interface {
	// Download triggers client to download file
	Download(*dfdaemonv1.DownRequest, dfdaemonv1.Daemon_DownloadServer) error
	// GetPieceTasks get piece tasks from other peers
	GetPieceTasks(context.Context, *commonv1.PieceTaskRequest) (*commonv1.PiecePacket, error)
	// SyncPieceTasks sync piece tasks info with other peers
	SyncPieceTasks(dfdaemonv1.Daemon_SyncPieceTasksServer) error
	// CheckHealth check daemon health
	CheckHealth(context.Context) error
	// Check if the given task exists in P2P cache system
	StatTask(context.Context, *dfdaemonv1.StatTaskRequest) error
	// Import the given file into P2P cache system
	ImportTask(context.Context, *dfdaemonv1.ImportTaskRequest) error
	// Export or download file from P2P cache system
	ExportTask(context.Context, *dfdaemonv1.ExportTaskRequest) error
	// Delete file from P2P cache system
	DeleteTask(context.Context, *dfdaemonv1.DeleteTaskRequest) error
}

type proxy struct {
	server DaemonServer
	dfdaemonv1.UnimplementedDaemonServer
}

func New(daemonServer DaemonServer, opts ...grpc.ServerOption) *grpc.Server {
	grpcServer := grpc.NewServer(append(rpc.DefaultServerOptions(), opts...)...)
	dfdaemonv1.RegisterDaemonServer(grpcServer, &proxy{server: daemonServer})
	return grpcServer
}

func (p *proxy) Download(req *dfdaemonv1.DownRequest, stream dfdaemonv1.Daemon_DownloadServer) (err error) {
	peerAddr := "unknown"
	if pe, ok := peer.FromContext(stream.Context()); ok {
		peerAddr = pe.Addr.String()
	}
	logger.Infof("trigger download for url: %s, from: %s, uuid: %s", req.Url, peerAddr, req.Uuid)
	return p.server.Download(req, stream)
}

func (p *proxy) GetPieceTasks(ctx context.Context, ptr *commonv1.PieceTaskRequest) (*commonv1.PiecePacket, error) {
	return p.server.GetPieceTasks(ctx, ptr)
}

func (p *proxy) SyncPieceTasks(sync dfdaemonv1.Daemon_SyncPieceTasksServer) error {
	return p.server.SyncPieceTasks(sync)
}

func (p *proxy) CheckHealth(ctx context.Context, req *emptypb.Empty) (*emptypb.Empty, error) {
	return new(emptypb.Empty), p.server.CheckHealth(ctx)
}

func (p *proxy) StatTask(ctx context.Context, req *dfdaemonv1.StatTaskRequest) (*emptypb.Empty, error) {
	return new(emptypb.Empty), p.server.StatTask(ctx, req)
}

func (p *proxy) ImportTask(ctx context.Context, req *dfdaemonv1.ImportTaskRequest) (*emptypb.Empty, error) {
	return new(emptypb.Empty), p.server.ImportTask(ctx, req)
}

func (p *proxy) ExportTask(ctx context.Context, req *dfdaemonv1.ExportTaskRequest) (*emptypb.Empty, error) {
	return new(emptypb.Empty), p.server.ExportTask(ctx, req)
}

func (p *proxy) DeleteTask(ctx context.Context, req *dfdaemonv1.DeleteTaskRequest) (*emptypb.Empty, error) {
	return new(emptypb.Empty), p.server.DeleteTask(ctx, req)
}
