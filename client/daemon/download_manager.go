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

package daemon

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	"google.golang.org/grpc"

	dfdaemongrpc "github.com/dragonflyoss/Dragonfly2/pkg/grpc/dfdaemon"
	"github.com/dragonflyoss/Dragonfly2/pkg/grpc/scheduler"
)

var _ dfdaemongrpc.DownloaderServer = &downloadManager{}

type DownloadManager interface {
	ServeGRPC(lis net.Listener) error
	ServeProxy(lis net.Listener) error
	Stop() error
}

type downloadManager struct {
	peerHost        *scheduler.PeerHost
	peerTaskManager PeerTaskManager

	grpcServer *grpc.Server
}

func NewDownloadManager(peerHost *scheduler.PeerHost, peerTaskManager PeerTaskManager) (DownloadManager, error) {
	mgr := &downloadManager{
		peerHost:        peerHost,
		peerTaskManager: peerTaskManager,
	}
	return mgr, nil
}

func (d *downloadManager) ServeGRPC(lis net.Listener) error {
	s := grpc.NewServer()
	dfdaemongrpc.RegisterDownloaderServer(s, d)
	d.grpcServer = s
	return s.Serve(lis)
}

func (d *downloadManager) ServeProxy(lis net.Listener) error {
	// TODO
	return nil
}

func (d *downloadManager) Stop() error {
	d.grpcServer.GracefulStop()
	// TODO stop proxy
	return nil
}

func (d *downloadManager) Download(req *dfdaemongrpc.DownloadRequest, server dfdaemongrpc.Downloader_DownloadServer) error {
	// init peer task request, peer download request uses different peer id
	peerTask := &FilePeerTaskRequest{
		PeerTaskRequest: scheduler.PeerTaskRequest{
			Url:      req.Url,
			Filter:   req.Filter,
			BizId:    req.BizId,
			UrlMata:  req.UrlMeta,
			Pid:      d.GenPeerID(),
			PeerHost: d.peerHost,
		},
		Output: req.Output,
	}

	peerTaskProgress, err := d.peerTaskManager.StartFilePeerTask(context.Background(), peerTask)
	if err != nil {
		return err
	}
	ctx := server.Context()
loop:
	for {
		select {
		case p, ok := <-peerTaskProgress:
			// FIXME is peer task done ?
			if !ok {
				break loop
			}
			err = server.Send(
				&dfdaemongrpc.DownloadResult{
					State:           p.State,
					TaskId:          p.TaskId,
					CompletedLength: p.CompletedLength,
					Done:            p.Done,
				})
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (d *downloadManager) GenPeerID() string {
	// FIXME review peer id format
	return fmt.Sprintf("%s-%d-%d-%d",
		d.peerHost.Ip, d.peerHost.Port, os.Getpid(), time.Now().UnixNano())
}
