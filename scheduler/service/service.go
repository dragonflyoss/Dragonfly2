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

	"go.opentelemetry.io/otel/trace"

	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/container/set"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/base/common"
	rpcscheduler "d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	pkgtime "d7y.io/dragonfly/v2/pkg/time"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/metrics"
	"d7y.io/dragonfly/v2/scheduler/resource"
	"d7y.io/dragonfly/v2/scheduler/scheduler"
	"d7y.io/dragonfly/v2/scheduler/storage"
)

type Service struct {
	// Resource interface.
	resource resource.Resource

	// Scheduler interface.
	scheduler scheduler.Scheduler

	// Scheduelr service config.
	config *config.Config

	// Dynamic config.
	dynconfig config.DynconfigInterface

	// Storage interface.
	storage storage.Storage
}

// New service instance.
func New(
	cfg *config.Config,
	resource resource.Resource,
	scheduler scheduler.Scheduler,
	dynconfig config.DynconfigInterface,
	storage storage.Storage,
) *Service {

	return &Service{
		resource:  resource,
		scheduler: scheduler,
		config:    cfg,
		dynconfig: dynconfig,
		storage:   storage,
	}
}

// RegisterPeerTask registers peer and triggers seed peer download task.
func (s *Service) RegisterPeerTask(ctx context.Context, req *rpcscheduler.PeerTaskRequest) (*rpcscheduler.RegisterResult, error) {
	// Register task and trigger seed peer download task.
	task, needBackToSource, err := s.registerTask(ctx, req)
	if err != nil {
		msg := fmt.Sprintf("peer %s register is failed: %s", req.PeerId, err.Error())
		logger.Error(msg)
		return nil, dferrors.New(base.Code_SchedTaskStatusError, msg)
	}
	host := s.registerHost(ctx, req.PeerHost)
	peer := s.registerPeer(ctx, req.PeerId, task, host, req.UrlMeta.Tag)
	peer.Log.Infof("register peer task request: %#v %#v %#v", req, req.UrlMeta, req.HostLoad)

	// When the peer registers for the first time and
	// does not have a seed peer, it will back-to-source.
	peer.NeedBackToSource.Store(needBackToSource)

	// The task state is TaskStateSucceeded and SizeScope is not invalid.
	sizeScope, err := task.SizeScope()
	if task.FSM.Is(resource.TaskStateSucceeded) && err == nil {
		peer.Log.Info("task can be reused")
		switch sizeScope {
		case base.SizeScope_TINY:
			peer.Log.Info("task size scope is tiny and return piece content directly")
			if len(task.DirectPiece) > 0 && int64(len(task.DirectPiece)) == task.ContentLength.Load() {
				if err := peer.FSM.Event(resource.PeerEventRegisterTiny); err != nil {
					msg := fmt.Sprintf("peer %s register is failed: %s", req.PeerId, err.Error())
					peer.Log.Error(msg)
					return nil, dferrors.New(base.Code_SchedError, msg)
				}

				return &rpcscheduler.RegisterResult{
					TaskId:    task.ID,
					TaskType:  task.Type,
					SizeScope: base.SizeScope_TINY,
					DirectPiece: &rpcscheduler.RegisterResult_PieceContent{
						PieceContent: task.DirectPiece,
					},
				}, nil
			}

			// Fallback to base.SizeScope_SMALL.
			peer.Log.Warnf("task size scope is tiny, length of direct piece is %d and content length is %d. fall through to size scope small",
				len(task.DirectPiece), task.ContentLength.Load())
			fallthrough
		case base.SizeScope_SMALL:
			peer.Log.Info("task size scope is small")
			// There is no need to build a tree, just find the parent and return.
			parent, ok := s.scheduler.FindParent(ctx, peer, set.NewSafeSet())
			if !ok {
				peer.Log.Warn("task size scope is small and it can not select parent")
				if err := peer.FSM.Event(resource.PeerEventRegisterNormal); err != nil {
					msg := fmt.Sprintf("peer %s register is failed: %s", req.PeerId, err.Error())
					peer.Log.Error(msg)
					return nil, dferrors.New(base.Code_SchedError, msg)
				}

				return &rpcscheduler.RegisterResult{
					TaskId:    task.ID,
					TaskType:  task.Type,
					SizeScope: base.SizeScope_NORMAL,
				}, nil
			}

			// When task size scope is small, parent must be downloaded successfully
			// before returning to the parent directly.
			if !parent.FSM.Is(resource.PeerStateSucceeded) {
				peer.Log.Infof("task size scope is small and download state %s is not PeerStateSucceeded", parent.FSM.Current())
				if err := peer.FSM.Event(resource.PeerEventRegisterNormal); err != nil {
					msg := fmt.Sprintf("peer %s register is failed: %s", req.PeerId, err.Error())
					peer.Log.Error(msg)
					return nil, dferrors.New(base.Code_SchedError, msg)
				}

				return &rpcscheduler.RegisterResult{
					TaskId:    task.ID,
					TaskType:  task.Type,
					SizeScope: base.SizeScope_NORMAL,
				}, nil
			}

			firstPiece, ok := task.LoadPiece(0)
			if !ok {
				peer.Log.Warn("task size scope is small and it can not get first piece")
				if err := peer.FSM.Event(resource.PeerEventRegisterNormal); err != nil {
					msg := fmt.Sprintf("peer %s register is failed: %s", req.PeerId, err.Error())
					peer.Log.Error(msg)
					return nil, dferrors.New(base.Code_SchedError, msg)
				}

				return &rpcscheduler.RegisterResult{
					TaskId:    task.ID,
					TaskType:  task.Type,
					SizeScope: base.SizeScope_NORMAL,
				}, nil
			}

			if err := peer.FSM.Event(resource.PeerEventRegisterSmall); err != nil {
				msg := fmt.Sprintf("peer %s register is failed: %s", req.PeerId, err.Error())
				peer.Log.Error(msg)
				return nil, dferrors.New(base.Code_SchedError, msg)
			}

			peer.ReplaceParent(parent)
			peer.Log.Infof("schedule parent successful, replace parent to %s ", parent.ID)
			peer.Log.Debugf("peer ancestors is %v", peer.Ancestors())

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
				TaskType:  task.Type,
				SizeScope: base.SizeScope_SMALL,
				DirectPiece: &rpcscheduler.RegisterResult_SinglePiece{
					SinglePiece: singlePiece,
				},
			}, nil
		default:
			peer.Log.Info("task size scope is normal and needs to be register")
			if err := peer.FSM.Event(resource.PeerEventRegisterNormal); err != nil {
				msg := fmt.Sprintf("peer %s register is failed: %s", req.PeerId, err.Error())
				peer.Log.Error(msg)
				return nil, dferrors.New(base.Code_SchedError, msg)
			}

			return &rpcscheduler.RegisterResult{
				TaskId:    task.ID,
				TaskType:  task.Type,
				SizeScope: base.SizeScope_NORMAL,
			}, nil
		}
	}

	// Task is unsuccessful.
	peer.Log.Infof("task state is %s and needs to be register", task.FSM.Current())
	if err := peer.FSM.Event(resource.PeerEventRegisterNormal); err != nil {
		msg := fmt.Sprintf("peer %s register is failed: %s", req.PeerId, err.Error())
		peer.Log.Error(msg)
		return nil, dferrors.New(base.Code_SchedError, msg)
	}

	return &rpcscheduler.RegisterResult{
		TaskId:    task.ID,
		TaskType:  task.Type,
		SizeScope: base.SizeScope_NORMAL,
	}, nil
}

// ReportPieceResult handles the piece information reported by dfdaemon.
func (s *Service) ReportPieceResult(stream rpcscheduler.Scheduler_ReportPieceResultServer) error {
	ctx := stream.Context()
	var (
		peer        *resource.Peer
		initialized bool
		ok          bool
	)

	for {
		select {
		case <-ctx.Done():
			logger.Infof("context was done")
			return ctx.Err()
		default:
		}

		piece, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			logger.Errorf("receive piece %#v error: %s", piece, err.Error())
			return err
		}

		if !initialized {
			initialized = true

			// Get peer from peer manager.
			peer, ok = s.resource.PeerManager().Load(piece.SrcPid)
			if !ok {
				msg := fmt.Sprintf("peer %s not found", piece.SrcPid)
				logger.Error(msg)
				return dferrors.New(base.Code_SchedPeerNotFound, msg)
			}

			// Peer setting stream.
			peer.StoreStream(stream)
			defer peer.DeleteStream()
		}

		if piece.PieceInfo != nil {
			// Handle begin of piece.
			if piece.PieceInfo.PieceNum == common.BeginOfPiece {
				peer.Log.Infof("receive begin of piece: %#v %#v", piece, piece.PieceInfo)
				s.handleBeginOfPiece(ctx, peer)
				continue
			}

			// Handle end of piece.
			if piece.PieceInfo.PieceNum == common.EndOfPiece {
				peer.Log.Infof("receive end of piece: %#v %#v", piece, piece.PieceInfo)
				s.handleEndOfPiece(ctx, peer)
				continue
			}
		}

		// Handle piece download successfully.
		if piece.Success {
			peer.Log.Infof("receive piece: %#v %#v", piece, piece.PieceInfo)
			s.handlePieceSuccess(ctx, peer, piece)

			// Collect peer host traffic metrics.
			if s.config.Metrics != nil && s.config.Metrics.EnablePeerHost {
				metrics.PeerHostTraffic.WithLabelValues(peer.BizTag, metrics.PeerHostTrafficDownloadType, peer.Host.ID, peer.Host.IP).Add(float64(piece.PieceInfo.RangeSize))
				if parent, ok := s.resource.PeerManager().Load(piece.DstPid); ok {
					metrics.PeerHostTraffic.WithLabelValues(peer.BizTag, metrics.PeerHostTrafficUploadType, parent.Host.ID, parent.Host.IP).Add(float64(piece.PieceInfo.RangeSize))
				} else {
					peer.Log.Warnf("dst peer %s not found for piece %#v %#v", piece.DstPid, piece, piece.PieceInfo)
				}
			}

			// Collect traffic metrics.
			if piece.DstPid != "" {
				metrics.Traffic.WithLabelValues(peer.BizTag, metrics.TrafficP2PType).Add(float64(piece.PieceInfo.RangeSize))
			} else {
				metrics.Traffic.WithLabelValues(peer.BizTag, metrics.TrafficBackToSourceType).Add(float64(piece.PieceInfo.RangeSize))
			}
			continue
		}

		// Handle piece download code.
		if piece.Code != base.Code_Success {
			if piece.Code == base.Code_ClientWaitPieceReady {
				peer.Log.Debugf("receive piece code %d and wait for dfdaemon piece ready", piece.Code)
				continue
			}

			// Handle piece download failed.
			peer.Log.Errorf("receive failed piece: %#v", piece)
			s.handlePieceFail(ctx, peer, piece)
			continue
		}

		peer.Log.Warnf("receive unknow piece: %#v %#v", piece, piece.PieceInfo)
	}
}

// ReportPeerResult handles peer result reported by dfdaemon.
func (s *Service) ReportPeerResult(ctx context.Context, req *rpcscheduler.PeerResult) error {
	peer, ok := s.resource.PeerManager().Load(req.PeerId)
	if !ok {
		msg := fmt.Sprintf("report peer result and peer %s is not exists", req.PeerId)
		logger.Error(msg)
		return dferrors.New(base.Code_SchedPeerNotFound, msg)
	}
	metrics.DownloadCount.WithLabelValues(peer.BizTag).Inc()

	if !req.Success {
		peer.Log.Errorf("report peer failed result: %s %#v", req.Code, req)
		if peer.FSM.Is(resource.PeerStateBackToSource) {
			s.createRecord(peer, storage.PeerStateBackToSourceFailed, req)
			metrics.DownloadFailureCount.WithLabelValues(peer.BizTag, metrics.DownloadFailureBackToSourceType).Inc()

			s.handleTaskFail(ctx, peer.Task)
			s.handlePeerFail(ctx, peer)
			return nil
		}

		s.createRecord(peer, storage.PeerStateFailed, req)
		metrics.DownloadFailureCount.WithLabelValues(peer.BizTag, metrics.DownloadFailureP2PType).Inc()

		s.handlePeerFail(ctx, peer)
		return nil
	}
	metrics.PeerTaskDownloadDuration.WithLabelValues(peer.BizTag).Observe(float64(req.Cost))

	peer.Log.Infof("report peer result: %#v", req)
	if peer.FSM.Is(resource.PeerStateBackToSource) {
		s.createRecord(peer, storage.PeerStateBackToSourceSucceeded, req)
		s.handleTaskSuccess(ctx, peer.Task, req)
		s.handlePeerSuccess(ctx, peer)
		return nil
	}

	s.createRecord(peer, storage.PeerStateSucceeded, req)
	s.handlePeerSuccess(ctx, peer)
	return nil
}

// StatTask checks the current state of the task.
func (s *Service) StatTask(ctx context.Context, req *rpcscheduler.StatTaskRequest) (*rpcscheduler.Task, error) {
	task, loaded := s.resource.TaskManager().Load(req.TaskId)
	if !loaded {
		msg := fmt.Sprintf("task %s not found", req.TaskId)
		logger.Info(msg)
		return nil, dferrors.New(base.Code_PeerTaskNotFound, msg)
	}

	task.Log.Debug("task has been found")
	return &rpcscheduler.Task{
		Id:               task.ID,
		Type:             task.Type,
		ContentLength:    task.ContentLength.Load(),
		TotalPieceCount:  task.TotalPieceCount.Load(),
		State:            task.FSM.Current(),
		PeerCount:        task.PeerCount.Load(),
		HasAvailablePeer: task.HasAvailablePeer(),
	}, nil
}

// AnnounceTask informs scheduler a peer has completed task.
func (s *Service) AnnounceTask(ctx context.Context, req *rpcscheduler.AnnounceTaskRequest) error {
	taskID := req.TaskId
	peerID := req.PiecePacket.DstPid

	task := resource.NewTask(taskID, req.Url, req.TaskType, req.UrlMeta)
	task, _ = s.resource.TaskManager().LoadOrStore(task)
	host := s.registerHost(ctx, req.PeerHost)
	peer := s.registerPeer(ctx, peerID, task, host, req.UrlMeta.Tag)
	peer.Log.Infof("announce peer task request: %#v %#v %#v %#v", req, req.UrlMeta, req.PeerHost, req.PiecePacket)

	// If the task state is not TaskStateSucceeded,
	// advance the task state to TaskStateSucceeded.
	if !task.FSM.Is(resource.TaskStateSucceeded) {
		if task.FSM.Can(resource.TaskEventDownload) {
			if err := task.FSM.Event(resource.TaskEventDownload); err != nil {
				msg := fmt.Sprintf("task fsm event failed: %s", err.Error())
				peer.Log.Error(msg)
				return dferrors.New(base.Code_SchedError, msg)
			}
		}

		// Load downloaded piece infos.
		for _, pieceInfo := range req.PiecePacket.PieceInfos {
			peer.Pieces.Set(uint(pieceInfo.PieceNum))
			peer.AppendPieceCost(int64(pieceInfo.DownloadCost) * int64(time.Millisecond))
			task.StorePiece(pieceInfo)
		}

		s.handleTaskSuccess(ctx, task, &rpcscheduler.PeerResult{
			TotalPieceCount: req.PiecePacket.TotalPiece,
			ContentLength:   req.PiecePacket.ContentLength,
		})
	}

	// If the peer state is not PeerStateSucceeded,
	// advance the peer state to PeerStateSucceeded.
	if !peer.FSM.Is(resource.PeerStateSucceeded) {
		if peer.FSM.Is(resource.PeerStatePending) {
			if err := peer.FSM.Event(resource.PeerEventRegisterNormal); err != nil {
				msg := fmt.Sprintf("peer fsm event failed: %s", err.Error())
				peer.Log.Error(msg)
				return dferrors.New(base.Code_SchedError, msg)
			}
		}

		if peer.FSM.Is(resource.PeerStateReceivedTiny) ||
			peer.FSM.Is(resource.PeerStateReceivedSmall) ||
			peer.FSM.Is(resource.PeerStateReceivedNormal) {
			if err := peer.FSM.Event(resource.PeerEventDownload); err != nil {
				msg := fmt.Sprintf("peer fsm event failed: %s", err.Error())
				peer.Log.Error(msg)
				return dferrors.New(base.Code_SchedError, msg)
			}

			s.handlePeerSuccess(ctx, peer)
		}
	}

	return nil
}

// LeaveTask makes the peer unschedulable.
func (s *Service) LeaveTask(ctx context.Context, req *rpcscheduler.PeerTarget) error {
	peer, ok := s.resource.PeerManager().Load(req.PeerId)
	if !ok {
		msg := fmt.Sprintf("leave task and peer %s is not exists", req.PeerId)
		logger.Error(msg)
		return dferrors.New(base.Code_SchedPeerNotFound, msg)
	}
	metrics.LeaveTaskCount.WithLabelValues(peer.BizTag).Inc()

	peer.Log.Infof("leave task: %#v", req)
	if err := peer.FSM.Event(resource.PeerEventLeave); err != nil {
		metrics.LeaveTaskFailureCount.WithLabelValues(peer.BizTag).Inc()

		msg := fmt.Sprintf("peer fsm event failed: %s", err.Error())
		peer.Log.Error(msg)
		return dferrors.New(base.Code_SchedTaskStatusError, msg)
	}

	peer.Children.Range(func(_, value interface{}) bool {
		child, ok := value.(*resource.Peer)
		if !ok {
			return true
		}

		// Reschedule a new parent to children of peer to exclude the current leave peer.
		child.Log.Infof("schedule parent because of parent peer %s is leaving", peer.ID)
		s.scheduler.ScheduleParent(ctx, child, child.BlockPeers)
		return true
	})

	s.resource.PeerManager().Delete(peer.ID)
	return nil
}

// registerTask creates a new task or reuses a previous task.
func (s *Service) registerTask(ctx context.Context, req *rpcscheduler.PeerTaskRequest) (*resource.Task, bool, error) {
	task := resource.NewTask(req.TaskId, req.Url, base.TaskType_Normal, req.UrlMeta, resource.WithBackToSourceLimit(int32(s.config.Scheduler.BackSourceCount)))
	task, loaded := s.resource.TaskManager().LoadOrStore(task)
	if loaded && !task.FSM.Is(resource.TaskStateFailed) {
		task.Log.Infof("task state is %s", task.FSM.Current())
		return task, false, nil
	}

	// Trigger task.
	if err := task.FSM.Event(resource.TaskEventDownload); err != nil {
		return nil, false, err
	}

	// Seed peer registers the task, then it needs to back-to-source.
	host, loaded := s.resource.HostManager().Load(req.PeerHost.Id)
	if loaded && host.Type != resource.HostTypeNormal {
		return task, true, nil
	}

	// Start trigger seed peer task.
	if s.config.SeedPeer.Enable {
		if task.IsSeedPeerFailed() {
			return task, true, nil
		}

		go s.triggerSeedPeerTask(ctx, task)
		return task, false, nil
	}

	// Task need to back-to-source.
	return task, true, nil
}

// registerHost creates a new host or reuses a previous host.
func (s *Service) registerHost(ctx context.Context, rawHost *rpcscheduler.PeerHost) *resource.Host {
	host, ok := s.resource.HostManager().Load(rawHost.Id)
	if !ok {
		// Get scheduler cluster client config by manager.
		var options []resource.HostOption
		if clientConfig, ok := s.dynconfig.GetSchedulerClusterClientConfig(); ok && clientConfig.LoadLimit > 0 {
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

// registerPeer creates a new peer or reuses a previous peer.
func (s *Service) registerPeer(ctx context.Context, peerID string, task *resource.Task, host *resource.Host, tag string) *resource.Peer {
	var options []resource.PeerOption
	if tag != "" {
		options = append(options, resource.WithBizTag(tag))
	}

	peer, loaded := s.resource.PeerManager().LoadOrStore(resource.NewPeer(peerID, task, host, options...))
	if !loaded {
		peer.Log.Info("create new peer")
		return peer
	}

	peer.Log.Infof("peer already exists, state %s", peer.FSM.Current())
	return peer
}

// triggerSeedPeerTask starts to trigger seed peer task.
func (s *Service) triggerSeedPeerTask(ctx context.Context, task *resource.Task) {
	task.Log.Infof("trigger seed peer download task and task status is %s", task.FSM.Current())
	peer, endOfPiece, err := s.resource.SeedPeer().TriggerTask(
		trace.ContextWithSpanContext(context.Background(), trace.SpanContextFromContext(ctx)), task)
	if err != nil {
		task.Log.Errorf("trigger seed peer download task failed: %s", err.Error())
		s.handleTaskFail(ctx, task)
		return
	}

	// Update the task status first to help peer scheduling evaluation and scoring.
	peer.Log.Info("trigger seed peer download task successfully")
	s.handleTaskSuccess(ctx, task, endOfPiece)
	s.handlePeerSuccess(ctx, peer)
}

// handleBeginOfPiece handles begin of piece.
func (s *Service) handleBeginOfPiece(ctx context.Context, peer *resource.Peer) {
	switch peer.FSM.Current() {
	case resource.PeerStateBackToSource:
		// Back to the source download process, peer directly returns.
		peer.Log.Info("peer downloads back-to-source when receive the begin of piece")
		return
	case resource.PeerStateReceivedTiny:
		// When the task is tiny,
		// the peer has already returned to piece data when registering.
		peer.Log.Info("file type is tiny, peer has already returned to piece data when registering")
		if err := peer.FSM.Event(resource.PeerEventDownload); err != nil {
			peer.Log.Errorf("peer fsm event failed: %s", err.Error())
			return
		}
	case resource.PeerStateReceivedSmall:
		// When the task is small,
		// the peer has already returned to the parent when registering.
		peer.Log.Info("file type is small, peer has already returned to the parent when registering")
		if err := peer.FSM.Event(resource.PeerEventDownload); err != nil {
			peer.Log.Errorf("peer fsm event failed: %s", err.Error())
			return
		}
	case resource.PeerStateReceivedNormal:
		if err := peer.FSM.Event(resource.PeerEventDownload); err != nil {
			peer.Log.Errorf("peer fsm event failed: %s", err.Error())
			return
		}

		peer.Log.Infof("schedule parent because of peer receive begin of piece")
		s.scheduler.ScheduleParent(ctx, peer, set.NewSafeSet())
	default:
		peer.Log.Warnf("peer state is %s when receive the begin of piece", peer.FSM.Current())
	}
}

// handleEndOfPiece handles end of piece.
func (s *Service) handleEndOfPiece(ctx context.Context, peer *resource.Peer) {}

// handlePieceSuccess handles successful piece.
func (s *Service) handlePieceSuccess(ctx context.Context, peer *resource.Peer, piece *rpcscheduler.PieceResult) {
	// Update peer piece info
	peer.Pieces.Set(uint(piece.PieceInfo.PieceNum))
	peer.AppendPieceCost(pkgtime.SubNano(int64(piece.EndTime), int64(piece.BeginTime)).Milliseconds())

	// When the peer downloads back-to-source,
	// piece downloads successfully updates the task piece info.
	if peer.FSM.Is(resource.PeerStateBackToSource) {
		peer.Task.StorePiece(piece.PieceInfo)
	}
}

// handlePieceFail handles failed piece.
func (s *Service) handlePieceFail(ctx context.Context, peer *resource.Peer, piece *rpcscheduler.PieceResult) {
	// Failed to download piece back-to-source.
	if peer.FSM.Is(resource.PeerStateBackToSource) {
		return
	}

	// If parent can not found, reschedule parent.
	parent, ok := s.resource.PeerManager().Load(piece.DstPid)
	if !ok {
		peer.Log.Errorf("schedule parent because of peer can not found parent %s", piece.DstPid)
		peer.BlockPeers.Add(piece.DstPid)
		s.scheduler.ScheduleParent(ctx, peer, peer.BlockPeers)
		return
	}

	// It’s not a case of back-to-source downloading failed,
	// to help peer to reschedule the parent node.
	switch piece.Code {
	case base.Code_PeerTaskNotFound:
		if err := parent.FSM.Event(resource.PeerEventDownloadFailed); err != nil {
			peer.Log.Errorf("peer fsm event failed: %s", err.Error())
			break
		}
	case base.Code_ClientPieceNotFound:
		// Dfdaemon downloading piece data from parent returns http error code 404.
		// If the parent is not a seed peer, reschedule parent for peer.
		// If the parent is a seed peer, scheduler need to trigger seed peer to download again.
		if parent.Host.Type == resource.HostTypeNormal {
			peer.Log.Infof("parent %s host type is normal", piece.DstPid)
			break
		}

		peer.Log.Infof("parent %s is seed peer", piece.DstPid)
		s.handleLegacySeedPeer(ctx, parent)

		// Start trigger seed peer task.
		if s.config.SeedPeer.Enable {
			go s.triggerSeedPeerTask(ctx, parent.Task)
		}
	default:
	}

	// Peer state is PeerStateRunning will be rescheduled.
	if !peer.FSM.Is(resource.PeerStateRunning) {
		peer.Log.Infof("peer can not be rescheduled because peer state is %s", peer.FSM.Current())
		return
	}

	peer.Log.Infof("schedule parent because of peer receive failed piece")
	peer.BlockPeers.Add(parent.ID)
	s.scheduler.ScheduleParent(ctx, peer, peer.BlockPeers)
}

// handlePeerSuccess handles successful peer.
func (s *Service) handlePeerSuccess(ctx context.Context, peer *resource.Peer) {
	if err := peer.FSM.Event(resource.PeerEventDownloadSucceeded); err != nil {
		peer.Log.Errorf("peer fsm event failed: %s", err.Error())
		return
	}

	sizeScope, err := peer.Task.SizeScope()
	if err != nil {
		peer.Log.Errorf("get task size scope failed: %s", err.Error())
		return
	}

	// If the peer type is tiny and back-to-source,
	// it need to directly download the tiny file and store the data in task DirectPiece.
	if sizeScope == base.SizeScope_TINY && len(peer.Task.DirectPiece) == 0 {
		data, err := peer.DownloadTinyFile()
		if err != nil {
			peer.Log.Errorf("download tiny task failed: %s", err.Error())
			return
		}

		if len(data) != int(peer.Task.ContentLength.Load()) {
			peer.Log.Errorf("download tiny task length of data is %d, task content length is %d", len(data), peer.Task.ContentLength.Load())
			return
		}

		// Tiny file downloaded successfully.
		peer.Task.DirectPiece = data
	}
}

// handlePeerFail handles failed peer.
func (s *Service) handlePeerFail(ctx context.Context, peer *resource.Peer) {
	if err := peer.FSM.Event(resource.PeerEventDownloadFailed); err != nil {
		peer.Log.Errorf("peer fsm event failed: %s", err.Error())
		return
	}

	// Reschedule a new parent to children of peer to exclude the current failed peer.
	peer.Children.Range(func(_, value interface{}) bool {
		child, ok := value.(*resource.Peer)
		if !ok {
			return true
		}

		child.Log.Infof("schedule parent because of parent peer %s is failed", peer.ID)
		s.scheduler.ScheduleParent(ctx, child, child.BlockPeers)
		return true
	})
}

// handleLegacySeedPeer handles seed server's task has left,
// but did not notify the schduler to leave the task.
func (s *Service) handleLegacySeedPeer(ctx context.Context, peer *resource.Peer) {
	if err := peer.FSM.Event(resource.PeerEventDownloadFailed); err != nil {
		peer.Log.Errorf("peer fsm event failed: %s", err.Error())
		return
	}

	if err := peer.FSM.Event(resource.PeerEventLeave); err != nil {
		peer.Log.Errorf("peer fsm event failed: %s", err.Error())
		return
	}

	// Reschedule a new parent to children of peer to exclude the current failed peer.
	peer.Children.Range(func(_, value interface{}) bool {
		child, ok := value.(*resource.Peer)
		if !ok {
			return true
		}

		child.Log.Infof("schedule parent because of parent peer %s is failed", peer.ID)
		s.scheduler.ScheduleParent(ctx, child, child.BlockPeers)
		return true
	})
}

// Conditions for the task to switch to the TaskStateSucceeded are:
// 1. Seed peer downloads the resource successfully.
// 2. Dfdaemon back-to-source to download successfully.
// 3. Peer announces it has the task.
func (s *Service) handleTaskSuccess(ctx context.Context, task *resource.Task, result *rpcscheduler.PeerResult) {
	if task.FSM.Is(resource.TaskStateSucceeded) {
		return
	}

	if err := task.FSM.Event(resource.TaskEventDownloadSucceeded); err != nil {
		task.Log.Errorf("task fsm event failed: %s", err.Error())
		return
	}

	// Update task's resource total piece count and content length.
	task.TotalPieceCount.Store(result.TotalPieceCount)
	task.ContentLength.Store(result.ContentLength)
}

// Conditions for the task to switch to the TaskStateSucceeded are:
// 1. Seed peer downloads the resource falied.
// 2. Dfdaemon back-to-source to download failed.
func (s *Service) handleTaskFail(ctx context.Context, task *resource.Task) {
	// If the number of failed peers in the task is greater than FailedPeerCountLimit,
	// then scheduler notifies running peers of failure.
	if task.PeerFailedCount.Load() > resource.FailedPeerCountLimit {
		task.PeerFailedCount.Store(0)
		task.NotifyPeers(base.Code_SchedTaskStatusError, resource.PeerEventDownloadFailed)
	}

	if task.FSM.Is(resource.TaskStateFailed) {
		return
	}

	if err := task.FSM.Event(resource.TaskEventDownloadFailed); err != nil {
		task.Log.Errorf("task fsm event failed: %s", err.Error())
		return
	}
}

// createRecord stores peer download records.
func (s *Service) createRecord(peer *resource.Peer, peerState int, req *rpcscheduler.PeerResult) {
	record := storage.Record{
		ID:              peer.ID,
		IP:              peer.Host.IP,
		Hostname:        peer.Host.Hostname,
		BizTag:          peer.BizTag,
		Cost:            req.Cost,
		PieceCount:      int32(peer.Pieces.Count()),
		TotalPieceCount: peer.Task.TotalPieceCount.Load(),
		ContentLength:   peer.Task.ContentLength.Load(),
		SecurityDomain:  peer.Host.SecurityDomain,
		IDC:             peer.Host.IDC,
		NetTopology:     peer.Host.NetTopology,
		Location:        peer.Host.Location,
		FreeUploadLoad:  peer.Host.FreeUploadLoad(),
		State:           peerState,
		HostType:        int(peer.Host.Type),
		CreateAt:        peer.CreateAt.Load().UnixNano(),
		UpdateAt:        peer.UpdateAt.Load().UnixNano(),
	}

	if parent, ok := peer.LoadParent(); ok {
		record.ParentID = parent.ID
		record.ParentIP = parent.Host.IP
		record.ParentHostname = parent.Host.Hostname
		record.ParentBizTag = parent.BizTag
		record.ParentPieceCount = int32(parent.Pieces.Count())
		record.ParentSecurityDomain = parent.Host.SecurityDomain
		record.ParentIDC = parent.Host.IDC
		record.ParentNetTopology = parent.Host.NetTopology
		record.ParentLocation = parent.Host.Location
		record.ParentFreeUploadLoad = parent.Host.FreeUploadLoad()
		record.ParentHostType = int(parent.Host.Type)
		record.ParentCreateAt = parent.CreateAt.Load().UnixNano()
		record.ParentUpdateAt = parent.UpdateAt.Load().UnixNano()
	}

	if err := s.storage.Create(record); err != nil {
		peer.Log.Error(err)
	}
}
