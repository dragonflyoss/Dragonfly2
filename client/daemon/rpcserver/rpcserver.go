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

//go:generate mockgen -destination mocks/rpcserver_mock.go -source rpcserver.go -package mocks

package rpcserver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"time"

	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	cdnsystemv1 "d7y.io/api/pkg/apis/cdnsystem/v1"
	commonv1 "d7y.io/api/pkg/apis/common/v1"
	dfdaemonv1 "d7y.io/api/pkg/apis/dfdaemon/v1"
	schedulerv1 "d7y.io/api/pkg/apis/scheduler/v1"
	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/peer"
	"d7y.io/dragonfly/v2/client/daemon/storage"
	"d7y.io/dragonfly/v2/client/util"
	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/net/http"
	dfdaemonserver "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/server"
	"d7y.io/dragonfly/v2/pkg/safe"
	"d7y.io/dragonfly/v2/scheduler/resource"
)

type Server interface {
	util.KeepAlive
	ServeDownload(listener net.Listener) error
	ServePeer(listener net.Listener) error
	Stop()
}

type server struct {
	util.KeepAlive
	peerHost        *schedulerv1.PeerHost
	peerTaskManager peer.TaskManager
	storageManager  storage.Manager
	defaultPattern  commonv1.Pattern

	downloadServer *grpc.Server
	peerServer     *grpc.Server
	uploadAddr     string
}

func New(peerHost *schedulerv1.PeerHost, peerTaskManager peer.TaskManager,
	storageManager storage.Manager, defaultPattern commonv1.Pattern,
	downloadOpts []grpc.ServerOption, peerOpts []grpc.ServerOption) (Server, error) {
	s := &server{
		KeepAlive:       util.NewKeepAlive("rpc server"),
		peerHost:        peerHost,
		peerTaskManager: peerTaskManager,
		storageManager:  storageManager,
		defaultPattern:  defaultPattern,
	}

	sd := &seeder{
		server: s,
	}

	s.downloadServer = dfdaemonserver.New(s, downloadOpts...)
	healthpb.RegisterHealthServer(s.downloadServer, health.NewServer())

	s.peerServer = dfdaemonserver.New(s, peerOpts...)
	healthpb.RegisterHealthServer(s.peerServer, health.NewServer())

	cdnsystemv1.RegisterSeederServer(s.peerServer, sd)
	return s, nil
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

func (s *server) GetPieceTasks(ctx context.Context, request *commonv1.PieceTaskRequest) (*commonv1.PiecePacket, error) {
	s.Keep()
	p, err := s.storageManager.GetPieces(ctx, request)
	if err != nil {
		code := commonv1.Code_UnknownError
		if err == dferrors.ErrInvalidArgument {
			code = commonv1.Code_BadRequest
		}
		if err != storage.ErrTaskNotFound {
			logger.Errorf("get piece tasks error: %s, task id: %s, src peer: %s, dst peer: %s, piece num: %d, limit: %d",
				err, request.TaskId, request.SrcPid, request.DstPid, request.StartNum, request.Limit)
			return nil, dferrors.New(code, err.Error())
		}
		// dst peer is not running
		task, ok := s.peerTaskManager.IsPeerTaskRunning(request.TaskId)
		if !ok {
			code = commonv1.Code_PeerTaskNotFound
			logger.Errorf("get piece tasks error: target peer task not found, task id: %s, src peer: %s, dst peer: %s, piece num: %d, limit: %d",
				request.TaskId, request.SrcPid, request.DstPid, request.StartNum, request.Limit)
			return nil, dferrors.New(code, err.Error())
		}

		if task.GetPeerID() != request.GetDstPid() {
			// there is only one running task in same time, redirect request to running peer task
			r := commonv1.PieceTaskRequest{
				TaskId:   request.TaskId,
				SrcPid:   request.SrcPid,
				DstPid:   task.GetPeerID(), // replace to running task peer id
				StartNum: request.StartNum,
				Limit:    request.Limit,
			}
			p, err = s.storageManager.GetPieces(ctx, &r)
			if err == nil {
				logger.Debugf("receive get piece tasks request, task id: %s, src peer: %s, dst peer: %s, replaced dst peer: %s, piece num: %d, limit: %d, length: %d",
					request.TaskId, request.SrcPid, request.DstPid, r.DstPid, request.StartNum, request.Limit, len(p.PieceInfos))
				p.DstAddr = s.uploadAddr
				return p, nil
			}
			code = commonv1.Code_PeerTaskNotFound
			logger.Errorf("get piece tasks error: target peer task and replaced peer task storage not found wit error: %s, task id: %s, src peer: %s, dst peer: %s, piece num: %d, limit: %d",
				err, request.TaskId, request.SrcPid, request.DstPid, request.StartNum, request.Limit)
			return nil, dferrors.New(code, err.Error())
		}

		logger.Infof("try to get piece tasks, "+
			"but target peer task is initializing, "+
			"there is no available pieces, "+
			"task id: %s, src peer: %s, dst peer: %s, piece num: %d, limit: %d",
			request.TaskId, request.SrcPid, request.DstPid, request.StartNum, request.Limit)
		// dst peer is running, send empty result, src peer will retry later
		return &commonv1.PiecePacket{
			TaskId:        request.TaskId,
			DstPid:        request.DstPid,
			DstAddr:       s.uploadAddr,
			PieceInfos:    nil,
			TotalPiece:    -1,
			ContentLength: -1,
			PieceMd5Sign:  "",
		}, nil
	}

	logger.Debugf("receive get piece tasks request, task id: %s, src peer: %s, dst peer: %s, piece start num: %d, limit: %d, count: %d, total content length: %d",
		request.TaskId, request.SrcPid, request.DstPid, request.StartNum, request.Limit, len(p.PieceInfos), p.ContentLength)
	p.DstAddr = s.uploadAddr
	return p, nil
}

// sendExistPieces will send as much as possible pieces
func (s *server) sendExistPieces(
	log *logger.SugaredLoggerOnWith,
	request *commonv1.PieceTaskRequest,
	sync dfdaemonv1.Daemon_SyncPieceTasksServer,
	get func(ctx context.Context, request *commonv1.PieceTaskRequest) (*commonv1.PiecePacket, error),
	sentMap map[int32]struct{}) (total int32, err error) {
	return sendExistPieces(sync.Context(), log, get, request, sync, sentMap, true)
}

// sendFirstPieceTasks will send as much as possible pieces, even if no available pieces
func (s *server) sendFirstPieceTasks(
	log *logger.SugaredLoggerOnWith,
	request *commonv1.PieceTaskRequest,
	sync dfdaemonv1.Daemon_SyncPieceTasksServer,
	get func(ctx context.Context, request *commonv1.PieceTaskRequest) (*commonv1.PiecePacket, error),
	sentMap map[int32]struct{}) (total int32, err error) {
	return sendExistPieces(sync.Context(), log, get, request, sync, sentMap, false)
}

func (s *server) SyncPieceTasks(sync dfdaemonv1.Daemon_SyncPieceTasksServer) error {
	request, err := sync.Recv()
	if err != nil {
		logger.Errorf("receive first sync piece tasks request error: %s", err.Error())
		return err
	}
	log := logger.With("taskID", request.TaskId,
		"localPeerID", request.DstPid, "remotePeerID", request.SrcPid)

	skipPieceCount := request.StartNum
	var (
		sentMap       = make(map[int32]struct{})
		attributeSent bool
	)

	getPieces := func(ctx context.Context, request *commonv1.PieceTaskRequest) (*commonv1.PiecePacket, error) {
		p, e := s.GetPieceTasks(ctx, request)
		if e != nil {
			return nil, e
		}
		p.DstAddr = s.uploadAddr
		if !attributeSent && len(p.PieceInfos) > 0 {
			exa, e := s.storageManager.GetExtendAttribute(ctx,
				&storage.PeerTaskMetadata{
					PeerID: request.DstPid,
					TaskID: request.TaskId,
				})
			if e != nil {
				log.Errorf("get extend attribute error: %s", e.Error())
				return nil, e
			}
			p.ExtendAttribute = exa
			attributeSent = true
		}
		return p, e
	}

	// TODO if not found, try to send to peer task conductor, then download it first
	total, err := s.sendFirstPieceTasks(log, request, sync, getPieces, sentMap)
	if err != nil {
		log.Errorf("send first piece tasks error: %s", err)
		return err
	}

	recvReminding := func() error {
		for {
			request, err = sync.Recv()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				logger.Errorf("receive reminding piece tasks request error: %s", err)
				return err
			}
			total, err = s.sendExistPieces(log, request, sync, getPieces, sentMap)
			if err != nil {
				logger.Errorf("send reminding piece tasks error: %s", err)
				return err
			}
		}
	}

	// task is done, just receive new piece tasks requests only
	if int(total) == len(sentMap)+int(skipPieceCount) {
		log.Infof("all piece tasks sent, receive new piece tasks requests only")
		return recvReminding()
	}

	// subscribe peer task message for remaining pieces
	result, ok := s.peerTaskManager.Subscribe(request)
	if !ok {
		// running task not found, double check for done task
		request.StartNum = searchNextPieceNum(sentMap, skipPieceCount)
		total, err = s.sendExistPieces(log, request, sync, getPieces, sentMap)
		if err != nil {
			log.Errorf("send exist piece tasks error: %s", err)
			return err
		}

		if int(total) > len(sentMap)+int(skipPieceCount) {
			return status.Errorf(codes.Unavailable, "peer task not finish, but no running task found")
		}
		return recvReminding()
	}

	var sub = &subscriber{
		SubscribeResponse:   result,
		sync:                sync,
		request:             request,
		skipPieceCount:      skipPieceCount,
		totalPieces:         total,
		sentMap:             sentMap,
		done:                make(chan struct{}),
		uploadAddr:          s.uploadAddr,
		SugaredLoggerOnWith: log,
		attributeSent:       atomic.NewBool(attributeSent),
	}

	go sub.receiveRemainingPieceTaskRequests()
	return sub.sendRemainingPieceTasks()
}

func (s *server) CheckHealth(context.Context) error {
	s.Keep()
	return nil
}

func (s *server) Download(ctx context.Context,
	req *dfdaemonv1.DownRequest, results chan<- *dfdaemonv1.DownResult) error {
	s.Keep()
	return s.doDownload(ctx, req, results, "")
}

func (s *server) doDownload(ctx context.Context, req *dfdaemonv1.DownRequest,
	results chan<- *dfdaemonv1.DownResult, peerID string) error {
	if req.UrlMeta == nil {
		req.UrlMeta = &commonv1.UrlMeta{}
	}

	// init peer task request, peer uses different peer id to generate every request
	// if peerID is not specified
	if peerID == "" {
		peerID = idgen.PeerID(s.peerHost.Ip)
	}
	peerTask := &peer.FileTaskRequest{
		PeerTaskRequest: schedulerv1.PeerTaskRequest{
			Url:      req.Url,
			UrlMeta:  req.UrlMeta,
			PeerId:   peerID,
			PeerHost: s.peerHost,
			Pattern:  config.ConvertPattern(req.Pattern, s.defaultPattern),
		},
		Output:             req.Output,
		Limit:              req.Limit,
		DisableBackSource:  req.DisableBackSource,
		Callsystem:         req.Callsystem,
		KeepOriginalOffset: req.KeepOriginalOffset,
	}
	if len(req.UrlMeta.Range) > 0 {
		r, err := http.ParseRange(req.UrlMeta.Range, math.MaxInt)
		if err != nil {
			err = fmt.Errorf("parse range %s error: %s", req.UrlMeta.Range, err)
			return err
		}
		peerTask.Range = &util.Range{
			Start:  int64(r.StartIndex),
			Length: int64(r.Length()),
		}
	}
	log := logger.With("peer", peerTask.PeerId, "component", "downloadService")

	peerTaskProgress, tiny, err := s.peerTaskManager.StartFileTask(ctx, peerTask)
	if err != nil {
		return dferrors.New(commonv1.Code_UnknownError, fmt.Sprintf("%s", err))
	}
	if tiny != nil {
		results <- &dfdaemonv1.DownResult{
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
				return dferrors.New(commonv1.Code_UnknownError, err.Error())
			}
			if !p.State.Success {
				log.Errorf("task %s/%s failed: %d/%s", p.PeerID, p.TaskID, p.State.Code, p.State.Msg)
				return dferrors.New(p.State.Code, p.State.Msg)
			}
			results <- &dfdaemonv1.DownResult{
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
			results <- &dfdaemonv1.DownResult{
				CompletedLength: 0,
				Done:            true,
			}
			log.Infof("context done due to %s", ctx.Err())
			return status.Error(codes.Canceled, ctx.Err().Error())
		}
	}
}

func (s *server) StatTask(ctx context.Context, req *dfdaemonv1.StatTaskRequest) error {
	s.Keep()
	taskID := idgen.TaskID(req.Url, req.UrlMeta)
	log := logger.With("function", "StatTask", "URL", req.Url, "Tag", req.UrlMeta.Tag, "taskID", taskID, "LocalOnly", req.LocalOnly)

	log.Info("new stat task request")
	if completed := s.isTaskCompleted(taskID); completed {
		log.Info("task found in local storage")
		return nil
	}

	// If only stat local cache and task doesn't exist, return not found
	if req.LocalOnly {
		msg := "task not found in local cache"
		log.Info(msg)
		return dferrors.New(commonv1.Code_PeerTaskNotFound, msg)
	}

	// Check scheduler if other peers hold the task
	task, se := s.peerTaskManager.StatTask(ctx, taskID)
	if se != nil {
		return se
	}
	// Task available for download only if task is in succeeded state and has available peer
	if task.State == resource.TaskStateSucceeded && task.HasAvailablePeer {
		return nil
	}
	msg := fmt.Sprintf("task found but not available for download, state %s, has available peer %t", task.State, task.HasAvailablePeer)
	log.Info(msg)
	return dferrors.New(commonv1.Code_PeerTaskNotFound, msg)
}

func (s *server) isTaskCompleted(taskID string) bool {
	return s.storageManager.FindCompletedTask(taskID) != nil
}

func (s *server) ImportTask(ctx context.Context, req *dfdaemonv1.ImportTaskRequest) error {
	s.Keep()
	peerID := idgen.PeerID(s.peerHost.Ip)
	taskID := idgen.TaskID(req.Url, req.UrlMeta)
	log := logger.With("function", "ImportTask", "URL", req.Url, "Tag", req.UrlMeta.Tag, "taskID", taskID, "file", req.Path)

	log.Info("new import task request")
	ptm := storage.PeerTaskMetadata{
		PeerID: peerID,
		TaskID: taskID,
	}
	announceFunc := func() {
		// TODO: retry announce on error
		start := time.Now()
		err := s.peerTaskManager.AnnouncePeerTask(context.Background(), ptm, req.Url, req.Type, req.UrlMeta)
		if err != nil {
			log.Warnf("Failed to announce task to scheduler: %s", err)
		} else {
			log.Infof("Announce task (peerID %s) to scheduler in %.6f seconds", ptm.PeerID, time.Since(start).Seconds())
		}
	}

	// 0. Task exists in local storage
	if task := s.storageManager.FindCompletedTask(taskID); task != nil {
		msg := fmt.Sprintf("import file skipped, task already exists with peerID %s", task.PeerID)
		log.Info(msg)

		// Announce to scheduler as well, but in background
		ptm.PeerID = task.PeerID
		go announceFunc()
		return nil
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
		return errors.New(msg)
	}

	// 2. Import task file
	pieceManager := s.peerTaskManager.GetPieceManager()
	if err := pieceManager.ImportFile(ctx, ptm, tsd, req); err != nil {
		msg := fmt.Sprintf("import file failed: %v", err)
		log.Error(msg)
		return errors.New(msg)
	}
	log.Info("import file succeeded")

	// 3. Announce to scheduler asynchronously
	go announceFunc()

	return nil
}

func (s *server) ExportTask(ctx context.Context, req *dfdaemonv1.ExportTaskRequest) error {
	s.Keep()
	taskID := idgen.TaskID(req.Url, req.UrlMeta)
	log := logger.With("function", "ExportTask", "URL", req.Url, "Tag", req.UrlMeta.Tag, "taskID", taskID, "destination", req.Output)

	log.Info("new export task request")
	task := s.storageManager.FindCompletedTask(taskID)
	if task == nil {
		// If only use local cache and task doesn't exist, return error
		if req.LocalOnly {
			msg := fmt.Sprintf("task not found in local storage")
			log.Info(msg)
			return dferrors.New(commonv1.Code_PeerTaskNotFound, msg)
		}
		log.Info("task not found, try from peers")
		return s.exportFromPeers(ctx, log, req)
	}
	err := s.exportFromLocal(ctx, req, task.PeerID)
	if err != nil {
		log.Errorf("export from local failed: %s", err)
		return err
	}
	return nil
}

func (s *server) exportFromLocal(ctx context.Context, req *dfdaemonv1.ExportTaskRequest, peerID string) error {
	return s.storageManager.Store(ctx, &storage.StoreRequest{
		CommonTaskRequest: storage.CommonTaskRequest{
			PeerID:      peerID,
			TaskID:      idgen.TaskID(req.Url, req.UrlMeta),
			Destination: req.Output,
		},
		StoreDataOnly: true,
	})
}

func (s *server) exportFromPeers(ctx context.Context, log *logger.SugaredLoggerOnWith, req *dfdaemonv1.ExportTaskRequest) error {
	peerID := idgen.PeerID(s.peerHost.Ip)
	taskID := idgen.TaskID(req.Url, req.UrlMeta)

	task, err := s.peerTaskManager.StatTask(ctx, taskID)
	if err != nil {
		if dferrors.CheckError(err, commonv1.Code_PeerTaskNotFound) {
			log.Info("task not found in P2P network")
		} else {
			msg := fmt.Sprintf("failed to StatTask from peers: %s", err)
			log.Error(msg)
		}
		return err
	}
	if task.State != resource.TaskStateSucceeded || !task.HasAvailablePeer {
		msg := fmt.Sprintf("task found but not available for download, state %s, has available peer %t", task.State, task.HasAvailablePeer)
		log.Info(msg)
		return dferrors.New(commonv1.Code_PeerTaskNotFound, msg)
	}

	// Task exists in peers
	var (
		start     = time.Now()
		drc       = make(chan *dfdaemonv1.DownResult, 1)
		errChan   = make(chan error, 3)
		result    *dfdaemonv1.DownResult
		downError error
	)
	downRequest := &dfdaemonv1.DownRequest{
		Url:               req.Url,
		Output:            req.Output,
		Timeout:           req.Timeout,
		Limit:             req.Limit,
		DisableBackSource: true,
		UrlMeta:           req.UrlMeta,
		Pattern:           "",
		Callsystem:        req.Callsystem,
		Uid:               req.Uid,
		Gid:               req.Gid,
	}

	go call(ctx, peerID, drc, s, downRequest, errChan)
	go func() {
		for result = range drc {
			if result.Done {
				log.Infof("export from peer successfully, length: %d bytes cost: %.6f s", result.CompletedLength, time.Since(start).Seconds())
				break
			}
		}
		errChan <- dferrors.ErrEndOfStream
	}()

	if downError = <-errChan; dferrors.IsEndOfStream(downError) {
		downError = nil
	}

	if downError != nil {
		msg := fmt.Sprintf("export from peer failed: %s", downError)
		log.Error(msg)
		return downError
	}
	return nil
}

func call(ctx context.Context, peerID string, drc chan *dfdaemonv1.DownResult, s *server, req *dfdaemonv1.DownRequest, errChan chan error) {
	err := safe.Call(func() {
		if err := s.doDownload(ctx, req, drc, peerID); err != nil {
			errChan <- err
		}
	})

	if err != nil {
		errChan <- err
	}
}

func (s *server) DeleteTask(ctx context.Context, req *dfdaemonv1.DeleteTaskRequest) error {
	s.Keep()
	taskID := idgen.TaskID(req.Url, req.UrlMeta)
	log := logger.With("function", "DeleteTask", "URL", req.Url, "Tag", req.UrlMeta.Tag, "taskID", taskID)

	log.Info("new delete task request")
	task := s.storageManager.FindCompletedTask(taskID)
	if task == nil {
		log.Info("task not found, skip delete")
		return nil
	}

	// Unregister task
	unregReq := storage.CommonTaskRequest{
		PeerID: task.PeerID,
		TaskID: taskID,
	}
	if err := s.storageManager.UnregisterTask(ctx, unregReq); err != nil {
		msg := fmt.Sprintf("failed to UnregisterTask: %s", err)
		log.Errorf(msg)
		return errors.New(msg)
	}
	return nil
}
