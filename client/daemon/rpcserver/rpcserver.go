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

package rpcserver

import (
	"context"
	"fmt"
	"math"
	"net"
	"os"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/client/daemon/peer"
	"d7y.io/dragonfly/v2/client/daemon/storage"
	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/base/common"
	dfdaemongrpc "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon"
	dfdaemonserver "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/server"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/pkg/util/rangeutils"
)

type Server interface {
	clientutil.KeepAlive
	ServeDownload(listener net.Listener) error
	ServePeer(listener net.Listener) error
	Stop()
}

type server struct {
	clientutil.KeepAlive
	peerHost        *scheduler.PeerHost
	peerTaskManager peer.TaskManager
	storageManager  storage.Manager

	downloadServer *grpc.Server
	peerServer     *grpc.Server
	uploadAddr     string
}

func New(peerHost *scheduler.PeerHost, peerTaskManager peer.TaskManager, storageManager storage.Manager, downloadOpts []grpc.ServerOption, peerOpts []grpc.ServerOption) (Server, error) {
	svr := &server{
		KeepAlive:       clientutil.NewKeepAlive("rpc server"),
		peerHost:        peerHost,
		peerTaskManager: peerTaskManager,
		storageManager:  storageManager,
	}

	svr.downloadServer = dfdaemonserver.New(svr, downloadOpts...)
	healthpb.RegisterHealthServer(svr.downloadServer, health.NewServer())

	svr.peerServer = dfdaemonserver.New(svr, peerOpts...)
	healthpb.RegisterHealthServer(svr.peerServer, health.NewServer())
	return svr, nil
}

func (s *server) ServeDownload(listener net.Listener) error {
	return s.downloadServer.Serve(listener)
}

func (s *server) ServePeer(listener net.Listener) error {
	s.uploadAddr = fmt.Sprintf("%s:%d", s.peerHost.Ip, s.peerHost.DownPort)
	return s.peerServer.Serve(listener)
}

func (s *server) Stop() {
	s.peerServer.GracefulStop()
	s.downloadServer.GracefulStop()
}

func (s *server) GetPieceTasks(ctx context.Context, request *base.PieceTaskRequest) (*base.PiecePacket, error) {
	s.Keep()
	p, err := s.storageManager.GetPieces(ctx, request)
	if err != nil {
		code := base.Code_UnknownError
		if err == dferrors.ErrInvalidArgument {
			code = base.Code_BadRequest
		}
		if err != storage.ErrTaskNotFound {
			logger.Errorf("get piece tasks error: %s, task id: %s, src peer: %s, dst peer: %s, piece num: %d, limit: %d",
				err, request.TaskId, request.SrcPid, request.DstPid, request.StartNum, request.Limit)
			return nil, dferrors.New(code, err.Error())
		}
		// dst peer is not running
		if !s.peerTaskManager.IsPeerTaskRunning(request.TaskId) {
			code = base.Code_PeerTaskNotFound
			logger.Errorf("get piece tasks error: peer task not found, task id: %s, src peer: %s, dst peer: %s, piece num: %d, limit: %d",
				request.TaskId, request.SrcPid, request.DstPid, request.StartNum, request.Limit)
			return nil, dferrors.New(code, err.Error())
		}

		logger.Infof("try to get piece tasks, "+
			"but target peer task is initializing, "+
			"there is no available pieces, "+
			"task id: %s, src peer: %s, dst peer: %s, piece num: %d, limit: %d",
			request.TaskId, request.SrcPid, request.DstPid, request.StartNum, request.Limit)
		// dst peer is running, send empty result, src peer will retry later
		return &base.PiecePacket{
			TaskId:        request.TaskId,
			DstPid:        request.DstPid,
			DstAddr:       s.uploadAddr,
			PieceInfos:    nil,
			TotalPiece:    -1,
			ContentLength: -1,
			PieceMd5Sign:  "",
		}, nil
	}

	logger.Debugf("receive get piece tasks request, task id: %s, src peer: %s, dst peer: %s, piece num: %d, limit: %d, length: %d",
		request.TaskId, request.SrcPid, request.DstPid, request.StartNum, request.Limit, len(p.PieceInfos))
	p.DstAddr = s.uploadAddr
	return p, nil
}

// sendExistPieces will send as much as possible pieces
func (s *server) sendExistPieces(request *base.PieceTaskRequest, sync dfdaemongrpc.Daemon_SyncPieceTasksServer, sentMap map[int32]struct{}) (total int32, sent int, err error) {
	return sendExistPieces(sync.Context(), s.GetPieceTasks, request, sync, sentMap, true)
}

// sendFirstPieceTasks will send as much as possible pieces, even if no available pieces
func (s *server) sendFirstPieceTasks(request *base.PieceTaskRequest, sync dfdaemongrpc.Daemon_SyncPieceTasksServer, sentMap map[int32]struct{}) (total int32, sent int, err error) {
	return sendExistPieces(sync.Context(), s.GetPieceTasks, request, sync, sentMap, false)
}

func (s *server) SyncPieceTasks(sync dfdaemongrpc.Daemon_SyncPieceTasksServer) error {
	request, err := sync.Recv()
	if err != nil {
		return err
	}
	skipPieceCount := request.StartNum
	var sentMap = make(map[int32]struct{})
	total, sent, err := s.sendFirstPieceTasks(request, sync, sentMap)
	if err != nil {
		return err
	}

	// task is done, just return
	if int(total) == sent {
		return nil
	}

	// subscribe peer task message for remaining pieces
	result, ok := s.peerTaskManager.Subscribe(request)
	if !ok {
		// task not found, double check for done task
		total, sent, err = s.sendExistPieces(request, sync, sentMap)
		if err != nil {
			return err
		}

		if int(total) > sent {
			return status.Errorf(codes.Unavailable, "peer task not finish, but no running task found")
		}
		return nil
	}

	var sub = &subscriber{
		SubscribeResult: result,
		sync:            sync,
		request:         request,
		skipPieceCount:  skipPieceCount,
		totalPieces:     total,
		sentMap:         sentMap,
		done:            make(chan struct{}),
		uploadAddr:      s.uploadAddr,
		SugaredLoggerOnWith: logger.With("taskID", request.TaskId,
			"localPeerID", request.DstPid, "remotePeerID", request.SrcPid),
	}

	go sub.receiveRemainingPieceTaskRequests()
	return sub.sendRemainingPieceTasks()
}

func (s *server) CheckHealth(context.Context) error {
	s.Keep()
	return nil
}

func (s *server) Download(ctx context.Context,
	req *dfdaemongrpc.DownRequest, results chan<- *dfdaemongrpc.DownResult) error {
	s.Keep()
	if req.UrlMeta == nil {
		req.UrlMeta = &base.UrlMeta{}
	}
	// init peer task request, peer uses different peer id to generate every request
	peerTask := &peer.FileTaskRequest{
		PeerTaskRequest: scheduler.PeerTaskRequest{
			Url:      req.Url,
			UrlMeta:  req.UrlMeta,
			PeerId:   idgen.PeerID(s.peerHost.Ip),
			PeerHost: s.peerHost,
		},
		Output:             req.Output,
		Limit:              req.Limit,
		DisableBackSource:  req.DisableBackSource,
		Pattern:            req.Pattern,
		Callsystem:         req.Callsystem,
		KeepOriginalOffset: req.KeepOriginalOffset,
	}
	if len(req.UrlMeta.Range) > 0 {
		r, err := rangeutils.ParseRange(req.UrlMeta.Range, math.MaxInt)
		if err != nil {
			err = fmt.Errorf("parse range %s error: %s", req.UrlMeta.Range, err)
			return err
		}
		peerTask.Range = &clientutil.Range{
			Start:  int64(r.StartIndex),
			Length: int64(r.Length()),
		}
	}
	log := logger.With("peer", peerTask.PeerId, "component", "downloadService")

	peerTaskProgress, tiny, err := s.peerTaskManager.StartFileTask(ctx, peerTask)
	if err != nil {
		return dferrors.New(base.Code_UnknownError, fmt.Sprintf("%s", err))
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
				return dferrors.New(base.Code_UnknownError, err.Error())
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

func (m *server) StatTask(ctx context.Context, req *dfdaemongrpc.StatTaskRequest) (*base.GrpcDfResult, error) {
	code := base.Code_Success
	msg := "task found in cache"
	taskID := idgen.TaskID(req.Cid, req.UrlMeta)
	log := logger.With("function", "StatTask", "Cid", req.Cid, "Tag", req.UrlMeta.Tag, "taskID", taskID, "LocalOnly", req.LocalOnly)

	log.Info("new stat task request")
	if completed := m.isTaskCompleted(taskID); !completed {
		// If only stat local cache and task doesn't exist, return not found
		if req.LocalOnly {
			msg = "task not found in local cache"
			log.Info(msg)
			return common.NewGrpcDfResult(base.Code_PeerTaskNotFound, msg), nil
		}
		res, err := m.peerTaskManager.StatPeerTask(ctx, taskID)
		if err != nil {
			msg = fmt.Sprintf("failed to StatPeerTask from peers: %v", err)
			log.Error(msg)
			return nil, errors.New(msg)
		}
		if res.Code == base.Code_PeerTaskNotFound {
			code = res.Code
			msg = res.Message
			log.Info(msg)
		}
	}
	return common.NewGrpcDfResult(code, msg), nil
}

func (m *server) ImportTask(ctx context.Context, req *dfdaemongrpc.ImportTaskRequest) (*base.GrpcDfResult, error) {
	peerID := idgen.PeerID(m.peerHost.Ip)
	taskID := idgen.TaskID(req.Cid, req.UrlMeta)
	log := logger.With("function", "ImportTask", "Cid", req.Cid, "Tag", req.UrlMeta.Tag, "taskID", taskID, "file", req.Path)

	log.Info("new import task request")
	ptm := storage.PeerTaskMetadata{
		PeerID: peerID,
		TaskID: taskID,
	}
	announceFunc := func() {
		// TODO: retry announce on error
		start := time.Now()
		err := m.peerTaskManager.AnnouncePeerTask(context.Background(), ptm, req.Cid, req.UrlMeta)
		if err != nil {
			log.Warnf("Failed to announce task to scheduler: %s", err)
		} else {
			log.Infof("Announce task (peerID %s) to scheduler in %.6f seconds", ptm.PeerID, time.Since(start).Seconds())
		}
	}

	// 0. Task exists in local storage
	if task := m.storageManager.FindCompletedTask(taskID); task != nil {
		msg := fmt.Sprintf("import file skipped, task already exists with peerID %s", task.PeerID)
		log.Info(msg)

		// Announce to scheduler as well, but in background
		ptm.PeerID = task.PeerID
		go announceFunc()
		return common.NewGrpcDfResult(base.Code_Success, msg), nil
	}

	// 1. Register to storageManager
	// TODO: compute and check hash digest if digest exists in ImportTaskRequest
	tsd, err := s.storageManager.RegisterTask(ctx, &storage.RegisterTaskRequest{
		PeerTaskMetadata: storage.PeerTaskMetadata{
			PeerID: peerID,
			TaskID: taskID,
		},
	})
	if err != nil {
		msg := fmt.Sprintf("register task to storage manager failed: %v", err)
		log.Error(msg)
		return nil, errors.New(msg)
	}

	// 2. Import task file
	pieceManager := m.peerTaskManager.GetPieceManager()
	if err := pieceManager.ImportFile(ctx, ptm, tsd, req); err != nil {
		msg := fmt.Sprintf("import file failed: %v", err)
		log.Error(msg)
		return nil, errors.New(msg)
	}
	log.Info("import file succeeded")

	// 3. Announce to scheduler asynchronously
	go announceFunc()

	return common.NewGrpcDfResult(base.Code_Success, "import file succeeded"), nil
}

func (m *server) isTaskCompleted(taskID string) bool {
	return m.storageManager.FindCompletedTask(taskID) != nil
}
