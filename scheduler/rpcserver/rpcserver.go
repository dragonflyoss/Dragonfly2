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

	"google.golang.org/grpc"

	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/container/set"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	schedulerserver "d7y.io/dragonfly/v2/pkg/rpc/scheduler/server"
	"d7y.io/dragonfly/v2/scheduler/resource"
	"d7y.io/dragonfly/v2/scheduler/service"
)

type Server struct {
	*grpc.Server
	service service.Service
}

// New returns a new transparent scheduler server from the given options
func New(service service.Service, opts ...grpc.ServerOption) *Server {
	svr := &Server{
		service: service,
	}

	svr.Server = schedulerserver.New(svr, opts...)
	return svr
}

func (s *Server) RegisterPeerTask(ctx context.Context, req *scheduler.PeerTaskRequest) (*scheduler.RegisterResult, error) {
	// Get task or add new task
	task, err := s.service.RegisterTask(ctx, req)
	if err != nil {
		dferr := dferrors.New(base.Code_SchedTaskStatusError, "register task is fail")
		logger.Errorf("peer %s register is failed: %v", req.PeerId, err)
		return nil, dferr
	}

	log := logger.WithTaskAndPeerID(task.ID, req.PeerId)
	log.Infof("register peer task request: %#v", req)

	// Task has been successful
	if task.FSM.Is(resource.TaskStateSucceeded) {
		log.Info("task has been successful")
		sizeScope := task.SizeScope()
		switch sizeScope {
		case base.SizeScope_TINY:
			log.Info("task size scope is tiny and return piece content directly")
			// When task.DirectPiece length is 0, data is downloaded by common peers failed
			if int64(len(task.DirectPiece)) == task.ContentLength.Load() {
				return &scheduler.RegisterResult{
					TaskId:    task.ID,
					SizeScope: sizeScope,
					DirectPiece: &scheduler.RegisterResult_PieceContent{
						PieceContent: task.DirectPiece,
					},
				}, nil
			}

			// Fallback to base.SizeScope_SMALL
			log.Warnf("task size scope is tiny, but task.DirectPiece length is %d, not %d. fall through to size scope small",
				len(task.DirectPiece), task.ContentLength.Load())
			fallthrough
		case base.SizeScope_SMALL:
			log.Info("task size scope is small")
			host, _ := s.service.LoadOrStoreHost(ctx, req)
			peer, _ := s.service.LoadOrStorePeer(ctx, req, task, host)

			// If the file is registered as a small type,
			// there is no need to build a tree, just find the parent and return
			parent, ok := s.service.Scheduler().FindParent(ctx, peer, set.NewSafeSet())
			if !ok {
				log.Warn("task size scope is small and it can not select parent")
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

			peer.ReplaceParent(parent)
			if err := peer.FSM.Event(resource.PeerEventRegisterSmall); err != nil {
				dferr := dferrors.New(base.Code_SchedError, err.Error())
				log.Errorf("peer %s register is failed: %v", req.PeerId, err)
				return nil, dferr
			}

			singlePiece := &scheduler.SinglePiece{
				DstPid:  parent.ID,
				DstAddr: fmt.Sprintf("%s:%d", parent.Host.IP, parent.Host.DownloadPort),
				PieceInfo: &base.PieceInfo{
					PieceNum:    firstPiece.PieceNum,
					RangeStart:  firstPiece.RangeStart,
					RangeSize:   firstPiece.RangeSize,
					PieceMd5:    firstPiece.PieceMd5,
					PieceOffset: firstPiece.PieceOffset,
					PieceStyle:  firstPiece.PieceStyle,
				},
			}

			log.Infof("task size scope is small and return single piece: %#v %#v", singlePiece, singlePiece.PieceInfo)
			return &scheduler.RegisterResult{
				TaskId:    task.ID,
				SizeScope: sizeScope,
				DirectPiece: &scheduler.RegisterResult_SinglePiece{
					SinglePiece: singlePiece,
				},
			}, nil
		default:
			log.Info("task size scope is normal and needs to be register")
			host, _ := s.service.LoadOrStoreHost(ctx, req)
			peer, _ := s.service.LoadOrStorePeer(ctx, req, task, host)
			if err := peer.FSM.Event(resource.PeerEventRegisterNormal); err != nil {
				dferr := dferrors.New(base.Code_SchedError, err.Error())
				log.Errorf("peer %s register is failed: %v", req.PeerId, err)
				return nil, dferr
			}

			return &scheduler.RegisterResult{
				TaskId:    task.ID,
				SizeScope: sizeScope,
			}, nil
		}
	}

	// Task is unsuccessful
	log.Info("task is unsuccessful and needs to be register")
	host, _ := s.service.LoadOrStoreHost(ctx, req)
	peer, _ := s.service.LoadOrStorePeer(ctx, req, task, host)
	if err := peer.FSM.Event(resource.PeerEventRegisterNormal); err != nil {
		dferr := dferrors.New(base.Code_SchedError, err.Error())
		log.Errorf("peer %s register is failed: %v", req.PeerId, err)
		return nil, dferr
	}

	return &scheduler.RegisterResult{
		TaskId:    task.ID,
		SizeScope: base.SizeScope_NORMAL,
	}, nil
}

func (s *Server) ReportPieceResult(stream scheduler.Scheduler_ReportPieceResultServer) error {
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

	// Get peer from peer manager
	peer, ok := s.service.LoadPeer(beginOfPiece.SrcPid)
	if !ok {
		dferr := dferrors.Newf(base.Code_SchedPeerNotFound, "peer %s not found", beginOfPiece.SrcPid)
		logger.Errorf("peer %s not found", beginOfPiece.SrcPid)
		return dferr
	}

	// Peer setting stream
	peer.StoreStream(stream)

	// Handle begin of piece
	s.service.HandlePiece(ctx, peer, beginOfPiece)

	for {
		select {
		case <-ctx.Done():
			peer.Log.Infof("context was done")
			return ctx.Err()
		case dferr := <-peer.StopChannel:
			peer.Log.Errorf("stream stop dferror: %v", dferr)
			return dferr
		default:
		}

		piece, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			peer.Log.Errorf("receive error: %v", err)
			return err
		}

		s.service.HandlePiece(ctx, peer, piece)
	}
}

func (s *Server) ReportPeerResult(ctx context.Context, req *scheduler.PeerResult) (err error) {
	peer, ok := s.service.LoadPeer(req.PeerId)
	if !ok {
		logger.Errorf("report peer result: peer %s is not exists", req.PeerId)
		return dferrors.Newf(base.Code_SchedPeerNotFound, "peer %s not found", req.PeerId)
	}

	peer.Log.Infof("report peer result request: %#v", req)
	s.service.HandlePeer(ctx, peer, req)
	return nil
}

func (s *Server) LeaveTask(ctx context.Context, req *scheduler.PeerTarget) (err error) {
	peer, ok := s.service.LoadPeer(req.PeerId)
	if !ok {
		logger.Errorf("leave task: peer %s is not exists", req.PeerId)
		return dferrors.Newf(base.Code_SchedPeerNotFound, "peer %s not found", req.PeerId)
	}

	peer.Log.Infof("leave task request: %#v", req)
	s.service.HandlePeerLeave(ctx, peer)
	return nil
}
