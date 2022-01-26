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
	"io"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/container/set"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/base/common"
	rpcscheduler "d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/metrics"
	"d7y.io/dragonfly/v2/scheduler/resource"
	"d7y.io/dragonfly/v2/scheduler/scheduler"
)

type Service interface {
	CDN() resource.CDN
	RegisterPeerTask(ctx context.Context, req *rpcscheduler.PeerTaskRequest) (resp *rpcscheduler.RegisterResult, err error)
	ReportPieceResult(stream rpcscheduler.Scheduler_ReportPieceResultServer) error
	ReportPeerResult(ctx context.Context, req *rpcscheduler.PeerResult) error
	LeaveTask(ctx context.Context, req *rpcscheduler.PeerTarget) error
}

type service struct {
	// Resource interface
	resource resource.Resource

	// Scheduler interface
	scheduler scheduler.Scheduler

	// callback holds some actions, like task fail, piece success actions
	callback Callback

	// Scheduelr service config
	config *config.Config

	// Dynamic config
	dynconfig config.DynconfigInterface
}

func New(
	cfg *config.Config,
	resource resource.Resource,
	scheduler scheduler.Scheduler,
	dynconfig config.DynconfigInterface,
) Service {
	// Initialize callback
	callback := newCallback(cfg, resource, scheduler)

	return &service{
		resource:  resource,
		scheduler: scheduler,
		callback:  callback,
		config:    cfg,
		dynconfig: dynconfig,
	}
}

func (s *service) CDN() resource.CDN {
	return s.resource.CDN()
}

func (s *service) RegisterPeerTask(ctx context.Context, req *rpcscheduler.PeerTaskRequest) (*rpcscheduler.RegisterResult, error) {
	// Register task and trigger cdn download task
	task, err := s.registerTask(ctx, req)
	if err != nil {
		dferr := status.Error(codes.Code(base.Code_SchedTaskStatusError), "register task is fail")
		logger.Errorf("peer %s register is failed: %v", req.PeerId, err)
		return nil, dferr
	}
	// Register host
	host := s.registerHost(ctx, req)
	// Register peer
	peer := s.registerPeer(ctx, req, task, host)
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

				return &rpcscheduler.RegisterResult{
					TaskId:    task.ID,
					SizeScope: base.SizeScope_TINY,
					DirectPiece: &rpcscheduler.RegisterResult_PieceContent{
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
			parent, ok := s.scheduler.FindParent(ctx, peer, set.NewSafeSet())
			if !ok {
				peer.Log.Warn("task size scope is small and it can not select parent")
				if err := peer.FSM.Event(resource.PeerEventRegisterNormal); err != nil {
					dferr := status.Error(codes.Code(base.Code_SchedError), err.Error())
					peer.Log.Errorf("peer %s register is failed: %v", req.PeerId, err)
					return nil, dferr
				}

				return &rpcscheduler.RegisterResult{
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

				return &rpcscheduler.RegisterResult{
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

			singlePiece := &rpcscheduler.SinglePiece{
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
			return &rpcscheduler.RegisterResult{
				TaskId:    task.ID,
				SizeScope: base.SizeScope_SMALL,
				DirectPiece: &rpcscheduler.RegisterResult_SinglePiece{
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

			return &rpcscheduler.RegisterResult{
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

	return &rpcscheduler.RegisterResult{
		TaskId:    task.ID,
		SizeScope: base.SizeScope_NORMAL,
	}, nil
}

func (s *service) ReportPieceResult(stream rpcscheduler.Scheduler_ReportPieceResultServer) error {
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
	peer, ok := s.resource.PeerManager().Load(beginOfPiece.SrcPid)
	if !ok {
		dferr := status.Error(codes.Code(base.Code_SchedPeerNotFound), err.Error())
		logger.Errorf("peer %s not found", beginOfPiece.SrcPid)
		return dferr
	}

	// Peer setting stream
	peer.StoreStream(stream)
	defer peer.DeleteStream()

	// Handle begin of piece
	s.handlePiece(ctx, peer, beginOfPiece)

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

		s.handlePiece(ctx, peer, piece)
	}
}

func (s *service) ReportPeerResult(ctx context.Context, req *rpcscheduler.PeerResult) error {
	peer, ok := s.resource.PeerManager().Load(req.PeerId)
	if !ok {
		logger.Errorf("report peer result: peer %s is not exists", req.PeerId)
		return status.Errorf(codes.Code(base.Code_SchedPeerNotFound), "peer %s not found", req.PeerId)
	}
	peer.Log.Infof("report peer result request: %#v", req)

	if !req.Success {
		if peer.Task.BackToSourcePeers.Contains(peer) {
			s.callback.TaskFail(ctx, peer.Task)
		}
		s.callback.PeerFail(ctx, peer)
		return nil
	}

	if peer.Task.BackToSourcePeers.Contains(peer) {
		s.callback.TaskSuccess(ctx, peer.Task, req)
	}
	s.callback.PeerSuccess(ctx, peer)
	return nil
}

func (s *service) LeaveTask(ctx context.Context, req *rpcscheduler.PeerTarget) error {
	peer, ok := s.resource.PeerManager().Load(req.PeerId)
	if !ok {
		logger.Errorf("leave task: peer %s is not exists", req.PeerId)
		return status.Errorf(codes.Code(base.Code_SchedPeerNotFound), "peer %s not found", req.PeerId)
	}

	peer.Log.Infof("leave task request: %#v", req)
	s.callback.PeerLeave(ctx, peer)
	return nil
}

func (s *service) registerTask(ctx context.Context, req *rpcscheduler.PeerTaskRequest) (*resource.Task, error) {
	task := resource.NewTask(idgen.TaskID(req.Url, req.UrlMeta), req.Url, s.config.Scheduler.BackSourceCount, req.UrlMeta)
	task, ok := s.resource.TaskManager().LoadOrStore(task)
	if ok && (task.FSM.Is(resource.TaskStateRunning) || task.FSM.Is(resource.TaskStateSucceeded)) {
		// Task is healthy and can be reused
		task.UpdateAt.Store(time.Now())
		task.Log.Infof("reuse task and status is %s", task.FSM.Current())
		return task, nil
	}

	// Trigger task
	if err := task.FSM.Event(resource.TaskEventDownload); err != nil {
		return nil, err
	}

	// Start seed cdn task
	go func() {
		task.Log.Infof("trigger cdn download task and task status is %s", task.FSM.Current())
		peer, endOfPiece, err := s.resource.CDN().TriggerTask(context.Background(), task)
		if err != nil {
			task.Log.Errorf("trigger cdn download task failed: %v", err)

			// Update the peer status first to help task return the error code to the peer that is downloading
			// If init cdn fails, peer is nil
			s.callback.TaskFail(ctx, task)
			return
		}

		// Update the task status first to help peer scheduling evaluation and scoring
		s.callback.TaskSuccess(ctx, task, endOfPiece)
		s.callback.PeerSuccess(ctx, peer)
	}()

	return task, nil
}

func (s *service) registerHost(ctx context.Context, req *rpcscheduler.PeerTaskRequest) *resource.Host {
	rawHost := req.PeerHost
	host, ok := s.resource.HostManager().Load(rawHost.Uuid)
	if !ok {
		// Get scheduler cluster client config by manager
		var options []resource.HostOption
		if clientConfig, ok := s.dynconfig.GetSchedulerClusterClientConfig(); ok {
			options = append(options, resource.WithUploadLoadLimit(int32(clientConfig.LoadLimit)))
		}

		host = resource.NewHost(rawHost, options...)
		s.resource.HostManager().Store(host)
		host.Log.Info("create new host")
		return host
	}

	host.Log.Info("host already exists")
	return host
}

func (s *service) registerPeer(ctx context.Context, req *rpcscheduler.PeerTaskRequest, task *resource.Task, host *resource.Host) *resource.Peer {
	peer, loaded := s.resource.PeerManager().LoadOrStore(resource.NewPeer(req.PeerId, task, host))
	if !loaded {
		peer.Log.Infof("create new peer")
	} else {
		peer.Log.Infof("peer already exists")
	}

	return peer
}

func (s *service) handlePiece(ctx context.Context, peer *resource.Peer, piece *rpcscheduler.PieceResult) {
	// Handle piece download successfully
	if piece.Success {
		peer.Log.Infof("receive successful piece: %#v %#v", piece, piece.PieceInfo)
		s.callback.PieceSuccess(ctx, peer, piece)

		// Collect peer host traffic metrics
		if s.config.Metrics != nil && s.config.Metrics.EnablePeerHost {
			metrics.PeerHostTraffic.WithLabelValues("download", peer.Host.ID, peer.Host.IP).Add(float64(piece.PieceInfo.RangeSize))
			if p, ok := s.resource.PeerManager().Load(piece.DstPid); ok {
				metrics.PeerHostTraffic.WithLabelValues("upload", p.Host.ID, p.Host.IP).Add(float64(piece.PieceInfo.RangeSize))
			} else {
				peer.Log.Warnf("dst peer %s not found for piece %#v %#v", piece.DstPid, piece, piece.PieceInfo)
			}
		}
		return
	}

	// Handle begin of piece and end of piece
	if piece.PieceInfo != nil {
		if piece.PieceInfo.PieceNum == common.BeginOfPiece {
			peer.Log.Infof("receive begin of piece: %#v %#v", piece, piece.PieceInfo)
			s.callback.BeginOfPiece(ctx, peer)
			return
		}

		if piece.PieceInfo.PieceNum == common.EndOfPiece {
			peer.Log.Infof("receive end of piece: %#v %#v", piece, piece.PieceInfo)
			s.callback.EndOfPiece(ctx, peer)
			return
		}
	}

	// Handle piece download code
	if piece.Code != base.Code_Success {
		// FIXME(244372610) When dfdaemon download peer return empty, retry later.
		if piece.Code == base.Code_ClientWaitPieceReady {
			peer.Log.Infof("receive piece code %d and wait for dfdaemon piece ready", piece.Code)
			return
		}

		// Handle piece download failed
		peer.Log.Errorf("receive failed piece: %#v %#v", piece, piece.PieceInfo)
		s.callback.PieceFail(ctx, peer, piece)
		return
	}

	// Handle unknow piece
	peer.Log.Warnf("receive unknow piece: %#v", piece)
	return
}
