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
	"os"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/client/daemon/peer"
	"d7y.io/dragonfly/v2/client/daemon/storage"
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/dferrors"
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
	peerTaskManager peer.TaskManager
	storageManager  storage.Manager

	downloadServer rpc.Server
	peerServer     rpc.Server
	uploadAddr     string
}

var _ dfdaemonserver.DaemonServer = (*manager)(nil)
var _ Manager = (*manager)(nil)

func NewManager(peerHost *scheduler.PeerHost, peerTaskManager peer.TaskManager, storageManager storage.Manager, downloadOpts []grpc.ServerOption, peerOpts []grpc.ServerOption) (Manager, error) {
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
		if err != storage.ErrTaskNotFound {
			logger.Errorf("get piece tasks error: %s, task id: %s, src peer: %s, dst peer: %s, piece num: %d, limit: %d",
				err, request.TaskId, request.SrcPid, request.DstPid, request.StartNum, request.Limit)
			return nil, dferrors.New(code, err.Error())
		}
		// dst peer is not running
		if !m.peerTaskManager.IsPeerTaskRunning(request.DstPid) {
			code = dfcodes.PeerTaskNotFound
			logger.Errorf("get piece tasks error: peer task not found, task id: %s, src peer: %s, dst peer: %s, piece num: %d, limit: %d",
				request.TaskId, request.SrcPid, request.DstPid, request.StartNum, request.Limit)
			return nil, dferrors.New(code, err.Error())
		}

		logger.Warnf("try to get piece tasks, "+
			"but target peer task is initializing, "+
			"there is no available pieces, "+
			"task id: %s, src peer: %s, dst peer: %s, piece num: %d, limit: %d",
			request.TaskId, request.SrcPid, request.DstPid, request.StartNum, request.Limit)
		// dst peer is running, send empty result, src peer will retry later
		return &base.PiecePacket{
			TaskId:        request.TaskId,
			DstPid:        request.DstPid,
			DstAddr:       m.uploadAddr,
			PieceInfos:    nil,
			TotalPiece:    -1,
			ContentLength: -1,
			PieceMd5Sign:  "",
		}, nil
	}

	logger.Debugf("receive get piece tasks request, task id: %s, src peer: %s, dst peer: %s, piece num: %d, limit: %d, length: %d",
		request.TaskId, request.SrcPid, request.DstPid, request.StartNum, request.Limit, len(p.PieceInfos))
	p.DstAddr = m.uploadAddr
	return p, nil
}

func (m *manager) CheckHealth(context.Context) error {
	m.Keep()
	return nil
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
			UrlMeta:  req.UrlMeta,
			PeerId:   clientutil.GenPeerID(m.peerHost),
			PeerHost: m.peerHost,
		},
		Output: req.Output,
	}
	log := logger.With("peer", peerTask.PeerId, "component", "downloadService")

	peerTaskProgress, tiny, err := m.peerTaskManager.StartFilePeerTask(ctx, peerTask)
	if err != nil {
		return dferrors.New(dfcodes.UnknownError, fmt.Sprintf("%s", err))
	}
	if tiny != nil {
		results <- &dfdaemongrpc.DownResult{
			TaskId:          tiny.TaskID,
			PeerId:          tiny.PeerID,
			CompletedLength: uint64(len(tiny.Content)),
			Done:            true,
		}
		log.Infof("tiny file, wrote to output")
		if req.Uid != 0 && req.Gid != 0 {
			if err = os.Chown(req.Output, int(req.Uid), int(req.Gid)); err != nil {
				log.Errorf("change own failed: %s", err)
				return err
			}
		}

		return nil
	}
	for {
		select {
		case p, ok := <-peerTaskProgress:
			if !ok {
				err = errors.New("progress closed unexpected")
				log.Errorf(err.Error())
				return dferrors.New(dfcodes.UnknownError, err.Error())
			}
			if !p.State.Success {
				log.Errorf("task %s/%s failed: %d/%s", p.PeerID, p.TaskID, p.State.Code, p.State.Msg)
				return dferrors.New(p.State.Code, p.State.Msg)
			}
			results <- &dfdaemongrpc.DownResult{
				TaskId:          p.TaskID,
				PeerId:          p.PeerID,
				CompletedLength: uint64(p.CompletedLength),
				Done:            p.PeerTaskDone,
			}
			// peer task sets PeerTaskDone to true only once
			if p.PeerTaskDone {
				p.DoneCallback()
				log.Infof("task %s/%s done", p.PeerID, p.TaskID)
				if req.Uid != 0 && req.Gid != 0 {
					log.Infof("change own to uid %d gid %d", req.Uid, req.Gid)
					if err = os.Chown(req.Output, int(req.Uid), int(req.Gid)); err != nil {
						log.Errorf("change own failed: %s", err)
						return err
					}
				}
				return nil
			}
		case <-ctx.Done():
			results <- &dfdaemongrpc.DownResult{
				CompletedLength: 0,
				Done:            true,
			}
			log.Infof("context done due to %s", ctx.Err())
			return status.Error(codes.Canceled, ctx.Err().Error())
		}
	}
}
