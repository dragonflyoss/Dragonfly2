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

package rpc

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	"google.golang.org/grpc"

	"github.com/dragonflyoss/Dragonfly2/client/daemon/peer"
	"github.com/dragonflyoss/Dragonfly2/client/daemon/storage"
	"github.com/dragonflyoss/Dragonfly2/pkg/basic"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
	dfdaemongrpc "github.com/dragonflyoss/Dragonfly2/pkg/rpc/dfdaemon"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler"
)

type Manager interface {
	ServeDaemon(network, address string) error
	ServeProxy(lis net.Listener) error
	Stop() error
}

type rpcManager struct {
	peerHost        *scheduler.PeerHost
	peerTaskManager peer.PeerTaskManager
	storageManager  storage.Manager

	grpcServer  *grpc.Server
	grpcOptions []grpc.ServerOption
}

func NewManager(peerHost *scheduler.PeerHost, peerTaskManager peer.PeerTaskManager, storageManager storage.Manager, opts ...grpc.ServerOption) (Manager, error) {
	mgr := &rpcManager{
		peerHost:        peerHost,
		peerTaskManager: peerTaskManager,
		storageManager:  storageManager,
		grpcOptions:     opts,
	}
	return mgr, nil
}

func (d *rpcManager) ServeDaemon(network, address string) error {
	logger.Infof("serve daemon at %s/%s", network, address)
	return rpc.StartServer(basic.NetAddr{
		Type: basic.NetworkType(network),
		Addr: address,
	}, d)
}

func (d *rpcManager) ServeProxy(lis net.Listener) error {
	// TODO
	return nil
}

func (d *rpcManager) Stop() error {
	rpc.StopServer()
	// TODO stop proxy
	return nil
}

func (d *rpcManager) GetPieceTasks(ctx context.Context, request *base.PieceTaskRequest) (*base.PiecePacket, error) {
	tasks, err := d.storageManager.GetPieces(ctx, request)
	if err != nil {
		return &base.PiecePacket{
			State: &base.ResponseState{
				Success: true,
				Code:    base.Code_CLIENT_ERROR,
				Msg:     fmt.Sprintf("get pieces error: %s", err),
			},
			TaskId: request.TaskId,
		}, nil
	}
	return &base.PiecePacket{
		State: &base.ResponseState{
			Success: true,
			Code:    base.Code_SUCCESS,
		},
		TaskId:     request.TaskId,
		PieceTasks: tasks,
	}, nil
}

func (d *rpcManager) CheckHealth(ctx context.Context, request *base.EmptyRequest) (*base.ResponseState, error) {
	return &base.ResponseState{
		Success: true,
		Code:    base.Code_SUCCESS,
		Msg:     "Running",
	}, nil
}

func (d *rpcManager) Download(ctx context.Context,
	req *dfdaemongrpc.DownRequest, results chan<- *dfdaemongrpc.DownResult) error {
	// init peer task request, peer download request uses different peer id
	peerTask := &peer.FilePeerTaskRequest{
		PeerTaskRequest: scheduler.PeerTaskRequest{
			Url:      req.Url,
			Filter:   req.Filter,
			BizId:    req.BizId,
			UrlMata:  req.UrlMeta,
			PeerId:   d.GenPeerID(),
			PeerHost: d.peerHost,
		},
		Output: req.Output,
	}

	peerTaskProgress, err := d.peerTaskManager.StartFilePeerTask(context.Background(), peerTask)
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
				CompletedLength: p.CompletedLength,
				Done:            p.Done,
			}
			if p.Done {
				break loop
			}
		case <-ctx.Done():
			results <- &dfdaemongrpc.DownResult{
				State: &base.ResponseState{
					Success: false,
					Code:    base.Code_REQUEST_TIME_OUT,
					Msg:     fmt.Sprintf("%s", ctx.Err()),
				},
				CompletedLength: 0,
				Done:            true,
			}
			return ctx.Err()
		}
	}
	return nil
}

func (d *rpcManager) GenPeerID() string {
	// FIXME review peer id format
	return fmt.Sprintf("%s-%d-%d-%d",
		d.peerHost.Ip, d.peerHost.Port, os.Getpid(), time.Now().UnixNano())
}
