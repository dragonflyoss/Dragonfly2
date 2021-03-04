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

package service

import (
	"context"
	"fmt"
	"net"

	"google.golang.org/grpc"

	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/client/daemon/peer"
	"d7y.io/dragonfly/v2/client/daemon/storage"
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	dfdaemongrpc "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon"
	dfdaemonserver "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/server"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
)

type Manager interface {
	clientutil.KeepAlive
	ServeDownload(listener net.Listener) error
	ServePeer(listener net.Listener) error
	Stop()
}

type manager struct {
	clientutil.KeepAlive
	peerHost        *scheduler.PeerHost
	peerTaskManager peer.PeerTaskManager
	storageManager  storage.Manager

	downloadServer rpc.Server
	peerServer     rpc.Server
	uploadAddr     string
}

var _ dfdaemonserver.DaemonServer = &manager{}

func NewManager(peerHost *scheduler.PeerHost, peerTaskManager peer.PeerTaskManager, storageManager storage.Manager, downloadOpts []grpc.ServerOption, peerOpts []grpc.ServerOption) (Manager, error) {
	mgr := &manager{
		KeepAlive:       clientutil.NewKeepAlive("service manager"),
		peerHost:        peerHost,
		peerTaskManager: peerTaskManager,
		storageManager:  storageManager,
	}
	mgr.downloadServer = rpc.NewServer(mgr, downloadOpts...)
	mgr.peerServer = rpc.NewServer(mgr, peerOpts...)
	return mgr, nil
}

func (m *manager) ServeDownload(listener net.Listener) error {
	return m.downloadServer.Serve(listener)
}

func (m *manager) ServePeer(listener net.Listener) error {
	m.uploadAddr = fmt.Sprintf("%s:%d", m.peerHost.Ip, m.peerHost.DownPort)
	return m.peerServer.Serve(listener)
}

func (m *manager) Stop() {
	m.peerServer.GracefulStop()
	m.downloadServer.GracefulStop()
}

func (m *manager) GetPieceTasks(ctx context.Context, request *base.PieceTaskRequest) (*base.PiecePacket, error) {
	m.Keep()
	p, err := m.storageManager.GetPieces(ctx, request)
	if err != nil {
		code := dfcodes.UnknownError
		if err == storage.ErrTaskNotFound {
			code = dfcodes.PeerTaskNotFound
		}
		logger.Errorf("receive get piece tasks request: %#v, error: %s, code: %v", request, err, code)
		return &base.PiecePacket{
			State: &base.ResponseState{
				Success: false,
				Code:    code,
				Msg:     fmt.Sprintf("%s", err),
			},
			TaskId: request.TaskId,
		}, nil
	}

	logger.Debugf("receive get piece tasks request: %#v, piece packet: %#v, length: %d", request, p, len(p.PieceInfos))
	p.DstAddr = m.uploadAddr
	return p, nil
}

func (m *manager) CheckHealth(context.Context) (*base.ResponseState, error) {
	m.Keep()
	return &base.ResponseState{
		Success: true,
		Code:    dfcodes.Success,
		Msg:     "Running",
	}, nil
}

func (m *manager) Download(ctx context.Context,
	req *dfdaemongrpc.DownRequest, results chan<- *dfdaemongrpc.DownResult) error {
	m.Keep()
	// init peer task request, peer download request uses different peer id
	peerTask := &peer.FilePeerTaskRequest{
		PeerTaskRequest: scheduler.PeerTaskRequest{
			Url:      req.Url,
			Filter:   req.Filter,
			BizId:    req.BizId,
			UrlMata:  req.UrlMeta,
			PeerId:   clientutil.GenPeerID(m.peerHost),
			PeerHost: m.peerHost,
		},
		Output: req.Output,
	}

	peerTaskProgress, err := m.peerTaskManager.StartFilePeerTask(ctx, peerTask)
	if err != nil {
		return err
	}
loop:
	for {
		select {
		case p := <-peerTaskProgress:
			results <- &dfdaemongrpc.DownResult{
				State:           p.State,
				TaskId:          p.TaskId,
				PeerId:          p.PeerID,
				CompletedLength: uint64(p.CompletedLength),
				Done:            p.Done,
			}
			if p.Done {
				logger.Infof("task %s done", p.TaskId)
				break loop
			}
		case <-ctx.Done():
			results <- &dfdaemongrpc.DownResult{
				State: &base.ResponseState{
					Success: false,
					Code:    dfcodes.RequestTimeOut,
					Msg:     fmt.Sprintf("%s", ctx.Err()),
				},
				CompletedLength: 0,
				Done:            true,
			}
			logger.Infof("context done due to %s", ctx.Err())
			return ctx.Err()
		}
	}
	return nil
}
