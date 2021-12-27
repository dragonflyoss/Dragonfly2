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
	"io"
	"net/url"

	"google.golang.org/grpc"

	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	schedulerserver "d7y.io/dragonfly/v2/pkg/rpc/scheduler/server"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/entity"
	"d7y.io/dragonfly/v2/scheduler/service"
)

type server struct {
	*grpc.Server
	service service.Service
	config  *config.Config
}

// New returns a new transparent scheduler server from the given options
func New(cfg *config.Config, service service.Service, opts ...grpc.ServerOption) (*grpc.Server, error) {
	svr := &server{
		service: service,
		config:  cfg,
	}

	svr.Server = schedulerserver.New(svr, opts...)
	return svr.Server, nil
}

func (s *server) RegisterPeerTask(ctx context.Context, req *scheduler.PeerTaskRequest) (*scheduler.RegisterResult, error) {
	// Get task or add new task
	task := s.service.RegisterTask(ctx, req, s.config.Scheduler.BackSourceCount)
	log := logger.WithTaskAndPeerID(task.ID, req.PeerId)
	log.Infof("register peer task request: %#v", req)

	// In the case of concurrency, if the task status is failure, it will directly return to registration failure.
	if task.FSM.Is(entity.TaskStateFailed) {
		dferr := dferrors.New(base.Code_SchedTaskStatusError, "task status is fail")
		log.Error(dferr.Message)
		return nil, dferr
	}

	// Task has been successful
	if task.FSM.Is(entity.TaskStateSucceeded) {
		log.Info("task has been successful")
		sizeScope := task.SizeScope()
		switch sizeScope {
		case base.SizeScope_TINY:
			log.Info("task size scope is tiny and return piece content directly")
			return &scheduler.RegisterResult{
				TaskId:    task.ID,
				SizeScope: sizeScope,
				DirectPiece: &scheduler.RegisterResult_PieceContent{
					PieceContent: task.DirectPiece,
				},
			}, nil
		case base.SizeScope_SMALL:
			log.Info("task size scope is small")
			host := s.service.LoadOrStoreHost(ctx, req)
			peer := s.service.LoadOrStorePeer(ctx, req, task, host)
			parent, _, ok := s.service.ScheduleParent(ctx, peer)
			if !ok {
				log.Warnf("task size scope is small and it can not select parent")
				return &scheduler.RegisterResult{
					TaskId:    task.ID,
					SizeScope: sizeScope,
				}, nil
			}

			firstPiece, ok := task.LoadPiece(0)
			if !ok {
				log.Warn("task size scope is small and it can not get first piece")
				return &scheduler.RegisterResult{
					TaskId:    task.ID,
					SizeScope: sizeScope,
				}, nil
			}

			dstURL := url.URL{
				Scheme: "http",
				Host:   fmt.Sprintf("%s:%d", parent.Host.IP, parent.Host.DownloadPort),
			}

			singlePiece := &scheduler.SinglePiece{
				DstPid:  parent.ID,
				DstAddr: dstURL.String(),
				PieceInfo: &base.PieceInfo{
					PieceNum:    firstPiece.PieceNum,
					RangeStart:  firstPiece.RangeStart,
					RangeSize:   firstPiece.RangeSize,
					PieceMd5:    firstPiece.PieceMd5,
					PieceOffset: firstPiece.PieceOffset,
					PieceStyle:  firstPiece.PieceStyle,
				},
			}
			log.Infof("task size scope is small and return single piece %#v", sizeScope)
			return &scheduler.RegisterResult{
				TaskId:    task.ID,
				SizeScope: sizeScope,
				DirectPiece: &scheduler.RegisterResult_SinglePiece{
					SinglePiece: singlePiece,
				},
			}, nil
		default:
			log.Info("task size scope is normal and needs to be register")
			host := s.service.LoadOrStoreHost(ctx, req)
			s.service.LoadOrStorePeer(ctx, req, task, host)
			return &scheduler.RegisterResult{
				TaskId:    task.ID,
				SizeScope: sizeScope,
			}, nil
		}
	}

	// Task is unsuccessful
	log.Info("task is unsuccessful and needs to be register")
	host := s.service.LoadOrStoreHost(ctx, req)
	s.service.LoadOrStorePeer(ctx, req, task, host)
	return &scheduler.RegisterResult{
		TaskId:    task.ID,
		SizeScope: base.SizeScope_NORMAL,
	}, nil
}

func (s *server) ReportPieceResult(stream scheduler.Scheduler_ReportPieceResultServer) error {
	ctx := stream.Context()

	// Handle begin of piece
	beginOfPiece, err := stream.Recv()
	if err != nil {
		if err == io.EOF {
			return nil
		}
		logger.Errorf("receive error: %v", err)
		return err
	}
	log := logger.WithTaskAndPeerID(beginOfPiece.TaskId, beginOfPiece.SrcPid)
	log.Infof("receive begin of piece: %#v", beginOfPiece)

	// Get peer from peer manager
	peer, ok := s.service.LoadPeer(beginOfPiece.SrcPid)
	if !ok {
		err = dferrors.Newf(base.Code_SchedPeerNotFound, "peer %s not found", beginOfPiece.SrcPid)
		log.Error("peer not found")
		return err
	}

	// Handle peer fails
	if peer.FSM.Is(entity.PeerStateFailed) {
		err = dferrors.Newf(base.Code_SchedTaskStatusError, "peer status is fail")
		log.Error("task status is fail")
		return err
	}

	// Peer setting stream
	peer.StoreStream(stream)
	for {
		select {
		case <-ctx.Done():
			log.Infof("context was done")
			return ctx.Err()
		case code := <-peer.StopChannel:
			log.Errorf("stream stop code: %v", code)
			return dferrors.New(code, "")
		default:
		}

		piece, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			log.Errorf("receive error: %v", err)
			return err
		}

		log.Infof("receive piece: %#v", piece)
		s.service.HandlePiece(ctx, peer, piece)
	}
}

func (s *server) ReportPeerResult(ctx context.Context, req *scheduler.PeerResult) (err error) {
	peer, ok := s.service.LoadPeer(req.PeerId)
	if !ok {
		logger.Errorf("report peer result: peer %s is not exists", req.PeerId)
		return dferrors.Newf(base.Code_SchedPeerNotFound, "peer %s not found", req.PeerId)
	}

	peer.Log.Infof("report peer result request: %#v", req)
	s.service.HandlePeer(ctx, peer, req)
	return nil
}

func (s *server) LeaveTask(ctx context.Context, req *scheduler.PeerTarget) (err error) {
	peer, ok := s.service.LoadPeer(req.PeerId)
	if !ok {
		logger.Errorf("leave task: peer %s is not exists", req.PeerId)
		return nil
	}

	peer.Log.Infof("leave tsk request: %#v", req)
	s.service.HandlePeerLeave(ctx, peer)
	return nil
}
