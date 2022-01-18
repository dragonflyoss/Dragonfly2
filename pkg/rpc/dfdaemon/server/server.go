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

package server

import (
	"context"

	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/dfdaemon"
)

// DaemonServer refer to dfdaemon.DaemonServer
type DaemonServer interface {
	// Download trigger client to download file
	Download(context.Context, *dfdaemon.DownRequest, dfdaemon.Daemon_DownloadServer) error
	// GetPieceTasks get piece tasks from other peers
	GetPieceTasks(context.Context, *base.PieceTaskRequest) (*base.PiecePacket, error)
	// CheckHealth check daemon health
	CheckHealth(context.Context) error
}

type proxy struct {
	server DaemonServer
	dfdaemon.UnimplementedDaemonServer
}

func New(daemonServer DaemonServer, opts ...grpc.ServerOption) *grpc.Server {
	grpcServer := grpc.NewServer(append(rpc.DefaultServerOptions, opts...)...)
	dfdaemon.RegisterDaemonServer(grpcServer, &proxy{server: daemonServer})
	return grpcServer
}

func (p *proxy) Download(req *dfdaemon.DownRequest, stream dfdaemon.Daemon_DownloadServer) (err error) {
	ctx := stream.Context()
	peerAddr := "unknown"
	if pe, ok := peer.FromContext(ctx); ok {
		peerAddr = pe.Addr.String()
	}
	logger.Infof("trigger download for url: %s, from: %s, uuid: %s", req.Url, peerAddr, req.Uuid)

	return p.server.Download(ctx, req, stream)
}

func (p *proxy) GetPieceTasks(ctx context.Context, ptr *base.PieceTaskRequest) (*base.PiecePacket, error) {
	return p.server.GetPieceTasks(ctx, ptr)
}

func (p *proxy) CheckHealth(ctx context.Context, req *empty.Empty) (*empty.Empty, error) {
	return new(empty.Empty), p.server.CheckHealth(ctx)
}
