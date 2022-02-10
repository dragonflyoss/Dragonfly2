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
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/types/known/emptypb"

	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/dfdaemon"
	"d7y.io/dragonfly/v2/pkg/safe"
)

//go:generate mockgen -source server.go -destination ../../../../client/daemon/test/mock/daemon/daemon_server.go
// DaemonServer refer to dfdaemon.DaemonServer
type DaemonServer interface {
	// Download triggers client to download file
	Download(context.Context, *dfdaemon.DownRequest, chan<- *dfdaemon.DownResult) error
	// GetPieceTasks get piece tasks from other peers
	GetPieceTasks(context.Context, *base.PieceTaskRequest) (*base.PiecePacket, error)
	// SyncPieceTasks sync piece tasks info with other peers
	SyncPieceTasks(dfdaemon.Daemon_SyncPieceTasksServer) error
	// CheckHealth check daemon health
	CheckHealth(context.Context) error
	// Check if the given task exists in P2P cache system
	StatTask(context.Context, *dfdaemon.StatTaskRequest) (*base.GrpcDfResult, error)
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
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	peerAddr := "unknown"
	if pe, ok := peer.FromContext(ctx); ok {
		peerAddr = pe.Addr.String()
	}
	logger.Infof("trigger download for url: %s, from: %s, uuid: %s", req.Url, peerAddr, req.Uuid)

	errChan := make(chan error, 10)
	drc := make(chan *dfdaemon.DownResult, 4)

	once := new(sync.Once)
	closeDrc := func() {
		once.Do(func() {
			close(drc)
		})
	}
	defer closeDrc()

	go call(ctx, drc, p, req, errChan)

	go send(drc, closeDrc, stream, errChan)

	if err = <-errChan; dferrors.IsEndOfStream(err) {
		err = nil
	}

	return
}

func (p *proxy) GetPieceTasks(ctx context.Context, ptr *base.PieceTaskRequest) (*base.PiecePacket, error) {
	return p.server.GetPieceTasks(ctx, ptr)
}

func (p *proxy) SyncPieceTasks(sync dfdaemon.Daemon_SyncPieceTasksServer) error {
	return p.server.SyncPieceTasks(sync)
}

func (p *proxy) CheckHealth(ctx context.Context, req *emptypb.Empty) (*emptypb.Empty, error) {
	return new(emptypb.Empty), p.server.CheckHealth(ctx)
}

func (p *proxy) StatTask(ctx context.Context, req *dfdaemon.StatTaskRequest) (*base.GrpcDfResult, error) {
	return p.server.StatTask(ctx, req)
}

func send(drc chan *dfdaemon.DownResult, closeDrc func(), stream dfdaemon.Daemon_DownloadServer, errChan chan error) {
	err := safe.Call(func() {
		defer closeDrc()

		for v := range drc {
			if err := stream.Send(v); err != nil {
				errChan <- err
				return
			}

			if v.Done {
				break
			}
		}

		errChan <- dferrors.ErrEndOfStream
	})

	if err != nil {
		errChan <- err
	}
}

func call(ctx context.Context, drc chan *dfdaemon.DownResult, p *proxy, req *dfdaemon.DownRequest, errChan chan error) {
	err := safe.Call(func() {
		if err := p.server.Download(ctx, req, drc); err != nil {
			errChan <- err
		}
	})

	if err != nil {
		errChan <- err
	}
}
