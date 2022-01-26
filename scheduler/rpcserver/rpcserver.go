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

	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/container/set"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/scheduler/metrics"
	"d7y.io/dragonfly/v2/scheduler/resource"
	"d7y.io/dragonfly/v2/scheduler/service"
)

type Server struct {
	*grpc.Server
	service service.Service
	scheduler.UnimplementedSchedulerServer
}

// New returns a new transparent scheduler server from the given options
func New(service service.Service, opts ...grpc.ServerOption) *Server {
	svr := &Server{
		service: service,
	}
	grpcServer := grpc.NewServer(append(rpc.DefaultServerOptions, opts...)...)
	scheduler.RegisterSchedulerServer(grpcServer, svr)
	svr.Server = grpcServer
	return svr
}

func (s *Server) RegisterPeerTask(ctx context.Context, req *scheduler.PeerTaskRequest) (resp *scheduler.RegisterResult, err error) {
	metrics.RegisterPeerTaskCount.Inc()
	defer func() {
		if err != nil {
			metrics.RegisterPeerTaskFailureCount.Inc()
		} else {
			metrics.PeerTaskCounter.WithLabelValues(resp.SizeScope.String()).Inc()
		}
	}()

	// Get task or add new task
	task, err := s.service.RegisterTask(ctx, req)
	if err != nil {
		dferr := status.Error(codes.Code(base.Code_SchedTaskStatusError), "register task is fail")
		logger.Errorf("peer %s register is failed: %v", req.PeerId, err)
		return nil, dferr
	}

	host, _ := s.service.LoadOrStoreHost(ctx, req)
	peer, _ := s.service.LoadOrStorePeer(ctx, req, task, host)
	peer.Log.Infof("register peer task request: %#v", req)

	// Task has been successful
	if task.FSM.Is(resource.TaskStateSucceeded) {
		peer.Log.Info("task has been successful")
		sizeScope := task.SizeScope()
		switch sizeScope {
		case base.SizeScope_TINY:
			peer.Log.Info("task size scope is tiny and return piece content directly")
			if len(task.DirectPiece) > 0 && int64(len(task.DirectPiece)) == task.ContentLength.Load() {
				if err := peer.FSM.Event(resource.PeerEventRegisterTiny); err != nil {
					dferr := status.Error(codes.Code(base.Code_SchedError), err.Error())
					peer.Log.Errorf("peer %s register is failed: %v", req.PeerId, err)
					return nil, dferr
				}

				// Dfdaemon does not report piece info when scope size is SizeScope_TINY
				if err := peer.FSM.Event(resource.PeerEventDownload); err != nil {
					dferr := status.Error(codes.Code(base.Code_SchedError), err.Error())
					peer.Log.Errorf("peer %s register is failed: %v", req.PeerId, err)
					return nil, dferr
				}

				return &scheduler.RegisterResult{
					TaskId:    task.ID,
					SizeScope: base.SizeScope_TINY,
					DirectPiece: &scheduler.RegisterResult_PieceContent{
						PieceContent: task.DirectPiece,
					},
				}, nil
			}

			// Fallback to base.SizeScope_SMALL
			peer.Log.Warnf("task size scope is tiny, length of direct piece is %d and content length is %d. fall through to size scope small",
				len(task.DirectPiece), task.ContentLength.Load())
			fallthrough
		case base.SizeScope_SMALL:
			peer.Log.Info("task size scope is small")
			// If the file is registered as a small type,
			// there is no need to build a tree, just find the parent and return
			parent, ok := s.service.Scheduler().FindParent(ctx, peer, set.NewSafeSet())
			if !ok {
				peer.Log.Warn("task size scope is small and it can not select parent")
				if err := peer.FSM.Event(resource.PeerEventRegisterNormal); err != nil {
					dferr := status.Error(codes.Code(base.Code_SchedError), err.Error())
					peer.Log.Errorf("peer %s register is failed: %v", req.PeerId, err)
					return nil, dferr
				}

				return &scheduler.RegisterResult{
					TaskId:    task.ID,
					SizeScope: base.SizeScope_NORMAL,
				}, nil
			}

			firstPiece, ok := task.LoadPiece(0)
			if !ok {
				peer.Log.Warn("task size scope is small and it can not get first piece")
				if err := peer.FSM.Event(resource.PeerEventRegisterNormal); err != nil {
					dferr := status.Error(codes.Code(base.Code_SchedError), err.Error())
					peer.Log.Errorf("peer %s register is failed: %v", req.PeerId, err)
					return nil, dferr
				}

				return &scheduler.RegisterResult{
					TaskId:    task.ID,
					SizeScope: base.SizeScope_NORMAL,
				}, nil
			}

			peer.ReplaceParent(parent)
			if err := peer.FSM.Event(resource.PeerEventRegisterSmall); err != nil {
				dferr := status.Error(codes.Code(base.Code_SchedError), err.Error())
				peer.Log.Errorf("peer %s register is failed: %v", req.PeerId, err)
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

			peer.Log.Infof("task size scope is small and return single piece: %#v %#v", singlePiece, singlePiece.PieceInfo)
			return &scheduler.RegisterResult{
				TaskId:    task.ID,
				SizeScope: base.SizeScope_SMALL,
				DirectPiece: &scheduler.RegisterResult_SinglePiece{
					SinglePiece: singlePiece,
				},
			}, nil
		default:
			peer.Log.Info("task size scope is normal and needs to be register")
			if err := peer.FSM.Event(resource.PeerEventRegisterNormal); err != nil {
				dferr := status.Error(codes.Code(base.Code_SchedError), err.Error())
				peer.Log.Errorf("peer %s register is failed: %v", req.PeerId, err)
				return nil, dferr
			}

			return &scheduler.RegisterResult{
				TaskId:    task.ID,
				SizeScope: base.SizeScope_NORMAL,
			}, nil
		}
	}

	// Task is unsuccessful
	peer.Log.Info("task is unsuccessful and needs to be register")
	if err := peer.FSM.Event(resource.PeerEventRegisterNormal); err != nil {
		dferr := status.Error(codes.Code(base.Code_SchedError), err.Error())
		peer.Log.Errorf("peer %s register is failed: %v", req.PeerId, err)
		return nil, dferr
	}

	return &scheduler.RegisterResult{
		TaskId:    task.ID,
		SizeScope: base.SizeScope_NORMAL,
	}, nil
}

func (s *Server) ReportPieceResult(stream scheduler.Scheduler_ReportPieceResultServer) error {
	metrics.ConcurrentScheduleGauge.Inc()
	defer metrics.ConcurrentScheduleGauge.Dec()
	ctx := stream.Context()
	peerAddr := "unknown"
	if pe, ok := peer.FromContext(ctx); ok {
		peerAddr = pe.Addr.String()
	}
	logger.Infof("start report piece from: %s", peerAddr)
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
		dferr := status.Errorf(codes.Code(base.Code_SchedPeerNotFound), "peer %s not found", beginOfPiece.SrcPid)
		logger.Errorf("peer %s not found", beginOfPiece.SrcPid)
		return dferr
	}

	// Peer setting stream
	peer.StoreStream(stream)
	defer peer.DeleteStream()

	// Handle begin of piece
	s.service.HandlePiece(ctx, peer, beginOfPiece)

	for {
		select {
		case <-ctx.Done():
			peer.Log.Infof("context was done")
			return ctx.Err()
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

func (s *Server) ReportPeerResult(ctx context.Context, req *scheduler.PeerResult) (*empty.Empty, error) {
	metrics.DownloadCount.Inc()
	if req.Success {
		metrics.P2PTraffic.Add(float64(req.Traffic))
		metrics.PeerTaskDownloadDuration.Observe(float64(req.Cost))
	} else {
		metrics.DownloadFailureCount.Inc()
	}
	peer, ok := s.service.LoadPeer(req.PeerId)
	if !ok {
		logger.Errorf("report peer result: peer %s is not exists", req.PeerId)
		return nil, status.Errorf(codes.Code(base.Code_SchedPeerNotFound), "peer %s not found", req.PeerId)
	}

	peer.Log.Infof("report peer result request: %#v", req)
	s.service.HandlePeer(ctx, peer, req)
	return new(empty.Empty), nil
}

func (s *Server) LeaveTask(ctx context.Context, req *scheduler.PeerTarget) (*empty.Empty, error) {
	peer, ok := s.service.LoadPeer(req.PeerId)
	if !ok {
		logger.Errorf("leave task: peer %s is not exists", req.PeerId)
		return nil, status.Errorf(codes.Code(base.Code_SchedPeerNotFound), "peer %s not found", req.PeerId)
	}

	peer.Log.Infof("leave task request: %#v", req)
	s.service.HandlePeerLeave(ctx, peer)
	return new(empty.Empty), nil
}
