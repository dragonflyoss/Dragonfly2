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
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/types/known/emptypb"

	commonv1 "d7y.io/api/pkg/apis/common/v1"
	dfdaemonv1 "d7y.io/api/pkg/apis/dfdaemon/v1"

	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/safe"
)

// DaemonServer refer to dfdaemonv1.DaemonServer
type DaemonServer interface {
	// Download triggers client to download file
	Download(context.Context, *dfdaemonv1.DownRequest, chan<- *dfdaemonv1.DownResult) error
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
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	peerAddr := "unknown"
	if pe, ok := peer.FromContext(ctx); ok {
		peerAddr = pe.Addr.String()
	}
	logger.Infof("trigger download for url: %s, from: %s, uuid: %s", req.Url, peerAddr, req.Uuid)

	errChan := make(chan error, 10)
	drc := make(chan *dfdaemonv1.DownResult, 4)

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

func send(drc chan *dfdaemonv1.DownResult, closeDrc func(), stream dfdaemonv1.Daemon_DownloadServer, errChan chan error) {
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

func call(ctx context.Context, drc chan *dfdaemonv1.DownResult, p *proxy, req *dfdaemonv1.DownRequest, errChan chan error) {
	err := safe.Call(func() {
		if err := p.server.Download(ctx, req, drc); err != nil {
			errChan <- err
		}
	})

	if err != nil {
		errChan <- err
	}
}
