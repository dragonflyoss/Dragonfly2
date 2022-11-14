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
	"errors"
	"fmt"
	"io"
	"net/url"
	"time"

	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/status"

	commonv1 "d7y.io/api/pkg/apis/common/v1"
	errordetailsv1 "d7y.io/api/pkg/apis/errordetails/v1"
	schedulerv1 "d7y.io/api/pkg/apis/scheduler/v1"

	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/container/set"
	"d7y.io/dragonfly/v2/pkg/rpc/common"
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
func (s *Service) RegisterPeerTask(ctx context.Context, req *schedulerv1.PeerTaskRequest) (*schedulerv1.RegisterResult, error) {
	logger.WithPeer(req.PeerHost.Id, req.TaskId, req.PeerId).Infof("register peer task request: %#v %#v %#v",
		req, req.UrlMeta, req.HostLoad)
	// Register task and trigger seed peer download task.
	task, needBackToSource := s.registerTask(ctx, req)
	host := s.registerHost(ctx, req.PeerHost)
	peer := s.registerPeer(ctx, req.PeerId, task, host, req.UrlMeta.Tag, req.UrlMeta.Application)

	// When the peer registers for the first time and
	// does not have a seed peer, it will back-to-source.
	peer.NeedBackToSource.Store(needBackToSource)

	sizeScope, err := task.SizeScope()
	if err != nil || !task.FSM.Is(resource.TaskStateSucceeded) {
		peer.Log.Infof("task can not be reused directly, because of task state is %s and %#v",
			task.FSM.Current(), err)
		result, err := s.registerNormalTask(ctx, peer)
		if err != nil {
			peer.Log.Error(err)
			return nil, dferrors.New(commonv1.Code_SchedError, err.Error())
		}

		return result, nil
	}

	// The task state is TaskStateSucceeded and SizeScope is not invalid.
	peer.Log.Info("task can be reused directly")
	switch sizeScope {
	case commonv1.SizeScope_EMPTY:
		peer.Log.Info("task size scope is EMPTY")
		result, err := s.registerEmptyTask(ctx, peer)
		if err != nil {
			peer.Log.Error(err)
			return nil, dferrors.New(commonv1.Code_SchedError, err.Error())
		}

		peer.Log.Info("return empty content")
		return result, nil
	case commonv1.SizeScope_TINY:
		peer.Log.Info("task size scope is TINY")

		// Validate data of direct piece.
		if !peer.Task.CanReuseDirectPiece() {
			peer.Log.Warnf("register as normal task, because of length of direct piece is %d, content length is %d",
				len(task.DirectPiece), task.ContentLength.Load())
			break
		}

		result, err := s.registerTinyTask(ctx, peer)
		if err != nil {
			peer.Log.Warnf("register as normal task, because of %s", err.Error())
			break
		}

		peer.Log.Info("return direct piece")
		return result, nil
	case commonv1.SizeScope_SMALL:
		peer.Log.Info("task size scope is SMALL")
		result, err := s.registerSmallTask(ctx, peer)
		if err != nil {
			peer.Log.Warnf("register as normal task, because of %s", err.Error())
			break
		}

		peer.Log.Info("return the single piece")
		return result, nil
	}

	peer.Log.Infof("task size scope is %s", sizeScope)
	result, err := s.registerNormalTask(ctx, peer)
	if err != nil {
		peer.Log.Error(err)
		return nil, dferrors.New(commonv1.Code_SchedError, err.Error())
	}

	peer.Log.Info("return the normal task")
	return result, nil
}

// ReportPieceResult handles the piece information reported by dfdaemon.
func (s *Service) ReportPieceResult(stream schedulerv1.Scheduler_ReportPieceResultServer) error {
	ctx := stream.Context()
	var (
		peer        *resource.Peer
		initialized bool
		loaded      bool
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
			peer, loaded = s.resource.PeerManager().Load(piece.SrcPid)
			if !loaded {
				msg := fmt.Sprintf("peer %s not found", piece.SrcPid)
				logger.Error(msg)
				return dferrors.New(commonv1.Code_SchedPeerNotFound, msg)
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
			if s.config.Metrics.Enable && s.config.Metrics.EnablePeerHost {
				metrics.PeerHostTraffic.WithLabelValues(peer.Tag, peer.Application, metrics.PeerHostTrafficDownloadType, peer.Host.ID, peer.Host.IP).Add(float64(piece.PieceInfo.RangeSize))
				if parent, loaded := s.resource.PeerManager().Load(piece.DstPid); loaded {
					metrics.PeerHostTraffic.WithLabelValues(peer.Tag, peer.Application, metrics.PeerHostTrafficUploadType, parent.Host.ID, parent.Host.IP).Add(float64(piece.PieceInfo.RangeSize))
				} else {
					peer.Log.Warnf("dst peer %s not found for piece %#v %#v", piece.DstPid, piece, piece.PieceInfo)
				}
			}

			// Collect traffic metrics.
			if piece.DstPid != "" {
				metrics.Traffic.WithLabelValues(peer.Tag, peer.Application, metrics.TrafficP2PType).Add(float64(piece.PieceInfo.RangeSize))
			} else {
				metrics.Traffic.WithLabelValues(peer.Tag, peer.Application, metrics.TrafficBackToSourceType).Add(float64(piece.PieceInfo.RangeSize))
			}
			continue
		}

		// Handle piece download code.
		if piece.Code != commonv1.Code_Success {
			if piece.Code == commonv1.Code_ClientWaitPieceReady {
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
func (s *Service) ReportPeerResult(ctx context.Context, req *schedulerv1.PeerResult) error {
	peer, loaded := s.resource.PeerManager().Load(req.PeerId)
	if !loaded {
		msg := fmt.Sprintf("report peer result and peer %s is not exists", req.PeerId)
		logger.Error(msg)
		return dferrors.New(commonv1.Code_SchedPeerNotFound, msg)
	}
	metrics.DownloadCount.WithLabelValues(peer.Tag, peer.Application).Inc()

	if !req.Success {
		peer.Log.Errorf("report peer failed result: %s %#v", req.Code, req)
		if peer.FSM.Is(resource.PeerStateBackToSource) {
			metrics.DownloadFailureCount.WithLabelValues(peer.Tag, peer.Application, metrics.DownloadFailureBackToSourceType).Inc()
			s.handleTaskFail(ctx, peer.Task, req.GetSourceError(), nil)
			s.handlePeerFail(ctx, peer)

			go s.createRecord(peer, storage.PeerStateBackToSourceFailed, req)
			return nil
		}

		go s.createRecord(peer, storage.PeerStateFailed, req)
		metrics.DownloadFailureCount.WithLabelValues(peer.Tag, peer.Application, metrics.DownloadFailureP2PType).Inc()

		s.handlePeerFail(ctx, peer)
		return nil
	}
	metrics.PeerTaskDownloadDuration.WithLabelValues(peer.Tag, peer.Application).Observe(float64(req.Cost))

	peer.Log.Infof("report peer result: %#v", req)
	if peer.FSM.Is(resource.PeerStateBackToSource) {
		s.handleTaskSuccess(ctx, peer.Task, req)
		s.handlePeerSuccess(ctx, peer)

		go s.createRecord(peer, storage.PeerStateBackToSourceSucceeded, req)
		return nil
	}

	s.handlePeerSuccess(ctx, peer)

	go s.createRecord(peer, storage.PeerStateSucceeded, req)
	return nil
}

// StatTask checks the current state of the task.
func (s *Service) StatTask(ctx context.Context, req *schedulerv1.StatTaskRequest) (*schedulerv1.Task, error) {
	task, loaded := s.resource.TaskManager().Load(req.TaskId)
	if !loaded {
		msg := fmt.Sprintf("task %s not found", req.TaskId)
		logger.Info(msg)
		return nil, dferrors.New(commonv1.Code_PeerTaskNotFound, msg)
	}

	task.Log.Debug("task has been found")
	return &schedulerv1.Task{
		Id:               task.ID,
		Type:             task.Type,
		ContentLength:    task.ContentLength.Load(),
		TotalPieceCount:  task.TotalPieceCount.Load(),
		State:            task.FSM.Current(),
		PeerCount:        int32(task.PeerCount()),
		HasAvailablePeer: task.HasAvailablePeer(),
	}, nil
}

// AnnounceTask informs scheduler a peer has completed task.
func (s *Service) AnnounceTask(ctx context.Context, req *schedulerv1.AnnounceTaskRequest) error {
	taskID := req.TaskId
	peerID := req.PiecePacket.DstPid

	task := resource.NewTask(taskID, req.Url, req.TaskType, req.UrlMeta)
	task, _ = s.resource.TaskManager().LoadOrStore(task)
	host := s.registerHost(ctx, req.PeerHost)
	peer := s.registerPeer(ctx, peerID, task, host, req.UrlMeta.Tag, req.UrlMeta.Application)
	peer.Log.Infof("announce peer task request: %#v %#v %#v %#v", req, req.UrlMeta, req.PeerHost, req.PiecePacket)

	// If the task state is not TaskStateSucceeded,
	// advance the task state to TaskStateSucceeded.
	if !task.FSM.Is(resource.TaskStateSucceeded) {
		if task.FSM.Can(resource.TaskEventDownload) {
			if err := task.FSM.Event(resource.TaskEventDownload); err != nil {
				msg := fmt.Sprintf("task fsm event failed: %s", err.Error())
				peer.Log.Error(msg)
				return dferrors.New(commonv1.Code_SchedError, msg)
			}
		}

		// Load downloaded piece infos.
		for _, pieceInfo := range req.PiecePacket.PieceInfos {
			peer.Pieces.Add(&schedulerv1.PieceResult{
				TaskId:          taskID,
				SrcPid:          peerID,
				DstPid:          req.PiecePacket.DstPid,
				Success:         true,
				PieceInfo:       pieceInfo,
				ExtendAttribute: req.PiecePacket.ExtendAttribute,
			})
			peer.FinishedPieces.Set(uint(pieceInfo.PieceNum))
			peer.AppendPieceCost(int64(pieceInfo.DownloadCost) * int64(time.Millisecond))
			task.StorePiece(pieceInfo)
		}

		s.handleTaskSuccess(ctx, task, &schedulerv1.PeerResult{
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
				return dferrors.New(commonv1.Code_SchedError, msg)
			}
		}

		if peer.FSM.Is(resource.PeerStateReceivedTiny) ||
			peer.FSM.Is(resource.PeerStateReceivedSmall) ||
			peer.FSM.Is(resource.PeerStateReceivedNormal) {
			if err := peer.FSM.Event(resource.PeerEventDownload); err != nil {
				msg := fmt.Sprintf("peer fsm event failed: %s", err.Error())
				peer.Log.Error(msg)
				return dferrors.New(commonv1.Code_SchedError, msg)
			}

			s.handlePeerSuccess(ctx, peer)
		}
	}

	return nil
}

// LeaveTask releases peer in scheduler.
func (s *Service) LeaveTask(ctx context.Context, req *schedulerv1.PeerTarget) error {
	peer, loaded := s.resource.PeerManager().Load(req.PeerId)
	if !loaded {
		msg := fmt.Sprintf("peer %s not found", req.PeerId)
		logger.Error(msg)
		return dferrors.New(commonv1.Code_SchedPeerNotFound, msg)
	}
	metrics.LeaveTaskCount.WithLabelValues(peer.Tag, peer.Application).Inc()

	peer.Log.Infof("client releases peer, causing the peer to leave: %#v", req)
	if err := peer.FSM.Event(resource.PeerEventLeave); err != nil {
		metrics.LeaveTaskFailureCount.WithLabelValues(peer.Tag, peer.Application).Inc()

		msg := fmt.Sprintf("peer fsm event failed: %s", err.Error())
		peer.Log.Error(msg)
		return dferrors.New(commonv1.Code_SchedTaskStatusError, msg)
	}

	return nil
}

// LeaveHost releases host in scheduler.
func (s *Service) LeaveHost(ctx context.Context, req *schedulerv1.LeaveHostRequest) error {
	logger.Infof("leave host: %#v", req)
	host, loaded := s.resource.HostManager().Load(req.Id)
	if !loaded {
		msg := fmt.Sprintf("host %s not found", req.Id)
		logger.Error(msg)
		return dferrors.New(commonv1.Code_BadRequest, msg)
	}

	host.LeavePeers()
	return nil
}

// registerTask creates a new task or reuses a previous task.
func (s *Service) registerTask(ctx context.Context, req *schedulerv1.PeerTaskRequest) (*resource.Task, bool) {
	task, loaded := s.resource.TaskManager().Load(req.TaskId)
	if loaded {
		// Task is the pointer, if the task already exists, the next request will
		// update the task's Url and UrlMeta in task manager.
		task.URL = req.Url
		task.URLMeta = req.UrlMeta

		if (task.FSM.Is(resource.TaskStatePending) || task.FSM.Is(resource.TaskStateRunning) || task.FSM.Is(resource.TaskStateSucceeded)) && task.HasAvailablePeer() {
			task.Log.Infof("task dose not need to back-to-source, because of task has available peer and state is %s", task.FSM.Current())
			return task, false
		}
	} else {
		// Create a task for the first time.
		task = resource.NewTask(req.TaskId, req.Url, commonv1.TaskType_Normal, req.UrlMeta, resource.WithBackToSourceLimit(int32(s.config.Scheduler.BackSourceCount)))
		s.resource.TaskManager().Store(task)
	}

	// If the task triggers the TaskEventDownload failed and it has no available peer,
	// let the peer do the scheduling.
	if err := task.FSM.Event(resource.TaskEventDownload); err != nil {
		task.Log.Warnf("task dose not need to back-to-source, because of %s", err.Error())
		return task, false
	}

	// Seed peer registers the task, then it needs to back-to-source.
	host, loaded := s.resource.HostManager().Load(req.PeerHost.Id)
	if loaded && host.Type != resource.HostTypeNormal {
		task.Log.Infof("task needs to back-to-source, because of host can be loaded and type is %d", host.Type)
		return task, true
	}

	// FIXME Need to add the condition that the seed peer grpc client is
	// available and can be triggered back-to-source.
	if s.config.SeedPeer.Enable {
		if task.IsSeedPeerFailed() {
			task.Log.Info("task needs to back-to-source, because of seed peer is failed")
			return task, true
		}

		go s.triggerSeedPeerTask(ctx, task)
		task.Log.Info("task dose not need to back-to-source, because of seed peer has been triggered")
		return task, false
	}

	// Task need to back-to-source.
	task.Log.Info("task needs to back-to-source, because of seed peer disabled")
	return task, true
}

// registerHost creates a new host or reuses a previous host.
func (s *Service) registerHost(ctx context.Context, rawHost *schedulerv1.PeerHost) *resource.Host {
	host, loaded := s.resource.HostManager().Load(rawHost.Id)
	if !loaded {
		// Get scheduler cluster client config by manager.
		var options []resource.HostOption
		if clientConfig, ok := s.dynconfig.GetSchedulerClusterClientConfig(); ok && clientConfig.LoadLimit > 0 {
			options = append(options, resource.WithConcurrentUploadLimit(int32(clientConfig.LoadLimit)))
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
func (s *Service) registerPeer(ctx context.Context, peerID string, task *resource.Task, host *resource.Host, tag, application string) *resource.Peer {
	var options []resource.PeerOption
	if tag != "" {
		options = append(options, resource.WithTag(tag))
	}
	if application != "" {
		options = append(options, resource.WithApplication(application))
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
		trace.ContextWithSpan(context.Background(), trace.SpanFromContext(ctx)), task)
	if err != nil {
		task.Log.Errorf("trigger seed peer download task failed: %s", err.Error())
		s.handleTaskFail(ctx, task, nil, err)
		return
	}

	// Update the task status first to help peer scheduling evaluation and scoring.
	peer.Log.Info("trigger seed peer download task successfully")
	s.handleTaskSuccess(ctx, task, endOfPiece)
	s.handlePeerSuccess(ctx, peer)
}

// registerEmptyTask registers the empty task.
func (s *Service) registerEmptyTask(ctx context.Context, peer *resource.Peer) (*schedulerv1.RegisterResult, error) {
	if err := peer.FSM.Event(resource.PeerEventRegisterEmpty); err != nil {
		return nil, err
	}

	return &schedulerv1.RegisterResult{
		TaskId:    peer.Task.ID,
		TaskType:  peer.Task.Type,
		SizeScope: commonv1.SizeScope_EMPTY,
		DirectPiece: &schedulerv1.RegisterResult_PieceContent{
			PieceContent: []byte{},
		},
	}, nil
}

// registerEmptyTask registers the tiny task.
func (s *Service) registerTinyTask(ctx context.Context, peer *resource.Peer) (*schedulerv1.RegisterResult, error) {
	if err := peer.FSM.Event(resource.PeerEventRegisterTiny); err != nil {
		return nil, err
	}

	return &schedulerv1.RegisterResult{
		TaskId:    peer.Task.ID,
		TaskType:  peer.Task.Type,
		SizeScope: commonv1.SizeScope_TINY,
		DirectPiece: &schedulerv1.RegisterResult_PieceContent{
			PieceContent: peer.Task.DirectPiece,
		},
	}, nil
}

// registerSmallTask registers the small task.
func (s *Service) registerSmallTask(ctx context.Context, peer *resource.Peer) (*schedulerv1.RegisterResult, error) {
	parent, ok := s.scheduler.FindParent(ctx, peer, set.NewSafeSet[string]())
	if !ok {
		return nil, errors.New("can not found parent")
	}

	// When task size scope is small, parent must be downloaded successfully
	// before returning to the parent directly.
	if !parent.FSM.Is(resource.PeerStateSucceeded) {
		return nil, fmt.Errorf("parent state %s is not PeerStateSucceede", parent.FSM.Current())
	}

	firstPiece, loaded := peer.Task.LoadPiece(0)
	if !loaded {
		return nil, fmt.Errorf("can not found first piece")
	}

	// Delete inedges of peer.
	if err := peer.Task.DeletePeerInEdges(peer.ID); err != nil {
		return nil, err
	}

	// Add edges between parent and peer.
	if err := peer.Task.AddPeerEdge(parent, peer); err != nil {
		return nil, err
	}

	if err := peer.FSM.Event(resource.PeerEventRegisterSmall); err != nil {
		return nil, err
	}

	return &schedulerv1.RegisterResult{
		TaskId:    peer.Task.ID,
		TaskType:  peer.Task.Type,
		SizeScope: commonv1.SizeScope_SMALL,
		DirectPiece: &schedulerv1.RegisterResult_SinglePiece{
			SinglePiece: &schedulerv1.SinglePiece{
				DstPid:  parent.ID,
				DstAddr: fmt.Sprintf("%s:%d", parent.Host.IP, parent.Host.DownloadPort),
				PieceInfo: &commonv1.PieceInfo{
					PieceNum:    firstPiece.PieceNum,
					RangeStart:  firstPiece.RangeStart,
					RangeSize:   firstPiece.RangeSize,
					PieceMd5:    firstPiece.PieceMd5,
					PieceOffset: firstPiece.PieceOffset,
					PieceStyle:  firstPiece.PieceStyle,
				},
			},
		},
	}, nil
}

// registerNormalTask registers the tiny task.
func (s *Service) registerNormalTask(ctx context.Context, peer *resource.Peer) (*schedulerv1.RegisterResult, error) {
	if err := peer.FSM.Event(resource.PeerEventRegisterNormal); err != nil {
		return nil, err
	}

	return &schedulerv1.RegisterResult{
		TaskId:    peer.Task.ID,
		TaskType:  peer.Task.Type,
		SizeScope: commonv1.SizeScope_NORMAL,
	}, nil
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
		s.scheduler.ScheduleParent(ctx, peer, set.NewSafeSet[string]())
	default:
		peer.Log.Warnf("peer state is %s when receive the begin of piece", peer.FSM.Current())
	}
}

// handleEndOfPiece handles end of piece.
func (s *Service) handleEndOfPiece(ctx context.Context, peer *resource.Peer) {}

// handlePieceSuccess handles successful piece.
func (s *Service) handlePieceSuccess(ctx context.Context, peer *resource.Peer, piece *schedulerv1.PieceResult) {
	// Update peer piece info.
	peer.Pieces.Add(piece)
	peer.FinishedPieces.Set(uint(piece.PieceInfo.PieceNum))
	peer.AppendPieceCost(pkgtime.SubNano(int64(piece.EndTime), int64(piece.BeginTime)).Milliseconds())

	// When the piece is downloaded successfully,
	// peer.UpdateAt needs to be updated to prevent
	// the peer from being GC during the download process.
	peer.UpdateAt.Store(time.Now())

	// When the peer downloads back-to-source,
	// piece downloads successfully updates the task piece info.
	if peer.FSM.Is(resource.PeerStateBackToSource) {
		peer.Task.StorePiece(piece.PieceInfo)
	}
}

// handlePieceFail handles failed piece.
func (s *Service) handlePieceFail(ctx context.Context, peer *resource.Peer, piece *schedulerv1.PieceResult) {
	// Failed to download piece back-to-source.
	if peer.FSM.Is(resource.PeerStateBackToSource) {
		return
	}

	// If parent can not found, reschedule parent.
	parent, loaded := s.resource.PeerManager().Load(piece.DstPid)
	if !loaded {
		peer.Log.Errorf("reschedule parent because of peer can not found parent %s", piece.DstPid)
		peer.BlockParents.Add(piece.DstPid)
		s.scheduler.ScheduleParent(ctx, peer, peer.BlockParents)
		return
	}

	// host upload failed and UploadErrorCount needs to be increased.
	parent.Host.UploadFailedCount.Inc()

	// It’s not a case of back-to-source downloading failed,
	// to help peer to reschedule the parent node.
	switch piece.Code {
	case commonv1.Code_PeerTaskNotFound:
		if err := parent.FSM.Event(resource.PeerEventDownloadFailed); err != nil {
			peer.Log.Errorf("peer fsm event failed: %s", err.Error())
			break
		}
	case commonv1.Code_ClientPieceNotFound:
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

		// Returns an scheduling error if the peer
		// state is not PeerStateRunning.
		stream, ok := peer.LoadStream()
		if !ok {
			peer.Log.Error("load stream failed")
			return
		}

		if err := stream.Send(&schedulerv1.PeerPacket{Code: commonv1.Code_SchedError}); err != nil {
			peer.Log.Errorf("send packet failed: %s", err.Error())
			return
		}

		return
	}

	peer.Log.Infof("reschedule parent because of peer receive failed piece")
	peer.BlockParents.Add(parent.ID)
	s.scheduler.ScheduleParent(ctx, peer, peer.BlockParents)
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
	// it needs to directly download the tiny file and store the data in task DirectPiece.
	if sizeScope == commonv1.SizeScope_TINY && len(peer.Task.DirectPiece) == 0 {
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
	for _, child := range peer.Children() {
		child.Log.Infof("reschedule parent because of parent peer %s is failed", peer.ID)
		s.scheduler.ScheduleParent(ctx, child, child.BlockParents)
	}
}

// handleLegacySeedPeer handles seed server's task has left,
// but did not notify the scheduler to leave the task.
func (s *Service) handleLegacySeedPeer(ctx context.Context, peer *resource.Peer) {
	peer.Log.Info("peer is legacy seed peer, causing the peer to leave")
	if err := peer.FSM.Event(resource.PeerEventLeave); err != nil {
		peer.Log.Errorf("peer fsm event failed: %s", err.Error())
		return
	}

	// Reschedule a new parent to children of peer to exclude the current failed peer.
	for _, child := range peer.Children() {
		child.Log.Infof("reschedule parent because of parent peer %s is failed", peer.ID)
		s.scheduler.ScheduleParent(ctx, child, child.BlockParents)
	}
}

// Conditions for the task to switch to the TaskStateSucceeded are:
// 1. Seed peer downloads the resource successfully.
// 2. Dfdaemon back-to-source to download successfully.
// 3. Peer announces it has the task.
func (s *Service) handleTaskSuccess(ctx context.Context, task *resource.Task, result *schedulerv1.PeerResult) {
	if task.FSM.Is(resource.TaskStateSucceeded) {
		return
	}

	// Update task's resource total piece count and content length.
	task.TotalPieceCount.Store(result.TotalPieceCount)
	task.ContentLength.Store(result.ContentLength)

	if err := task.FSM.Event(resource.TaskEventDownloadSucceeded); err != nil {
		task.Log.Errorf("task fsm event failed: %s", err.Error())
		return
	}
}

// Conditions for the task to switch to the TaskStateSucceeded are:
// 1. Seed peer downloads the resource failed.
// 2. Dfdaemon back-to-source to download failed.
func (s *Service) handleTaskFail(ctx context.Context, task *resource.Task, backToSourceErr *errordetailsv1.SourceError, seedPeerErr error) {
	// If peer back-to-source fails due to an unrecoverable error,
	// notify other peers of the failure,
	// and return the source metadata to peer.
	if backToSourceErr != nil {
		if !backToSourceErr.Temporary {
			task.NotifyPeers(&schedulerv1.PeerPacket{
				Code: commonv1.Code_BackToSourceAborted,
				Errordetails: &schedulerv1.PeerPacket_SourceError{
					SourceError: backToSourceErr,
				},
			}, resource.PeerEventDownloadFailed)
			task.PeerFailedCount.Store(0)
		}
	} else if seedPeerErr != nil {
		// If seed peer back-to-source fails due to an unrecoverable error,
		// notify other peers of the failure,
		// and return the source metadata to peer.
		if st, ok := status.FromError(seedPeerErr); ok {
			for _, detail := range st.Details() {
				switch d := detail.(type) {
				case *errordetailsv1.SourceError:
					var proto = "unknown"
					if u, err := url.Parse(task.URL); err == nil {
						proto = u.Scheme
					}
					// TODO currently, metrics.PeerTaskSourceErrorCounter is only updated for seed peer source error, need update for normal peer
					if d.Metadata != nil {
						task.Log.Infof("source error: %d/%s", d.Metadata.StatusCode, d.Metadata.Status)
						metrics.PeerTaskSourceErrorCounter.WithLabelValues(
							task.URLMeta.Tag, task.URLMeta.Application, proto, fmt.Sprintf("%d", d.Metadata.StatusCode)).Inc()
					} else {
						task.Log.Warn("source error, but no metadata found")
						metrics.PeerTaskSourceErrorCounter.WithLabelValues(
							task.URLMeta.Tag, task.URLMeta.Application, proto, "0").Inc()
					}
					if !d.Temporary {
						task.Log.Infof("source error is not temporary, notify other peers task aborted")
						task.NotifyPeers(&schedulerv1.PeerPacket{
							Code: commonv1.Code_BackToSourceAborted,
							Errordetails: &schedulerv1.PeerPacket_SourceError{
								SourceError: d,
							},
						}, resource.PeerEventDownloadFailed)
						task.PeerFailedCount.Store(0)
					}
				}
			}
		}
	} else if task.PeerFailedCount.Load() > resource.FailedPeerCountLimit {
		// If the number of failed peers in the task is greater than FailedPeerCountLimit,
		// then scheduler notifies running peers of failure.
		task.NotifyPeers(&schedulerv1.PeerPacket{
			Code: commonv1.Code_SchedTaskStatusError,
		}, resource.PeerEventDownloadFailed)
		task.PeerFailedCount.Store(0)
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
func (s *Service) createRecord(peer *resource.Peer, peerState int, req *schedulerv1.PeerResult) {
	parent, err := peer.MainParent()
	if err != nil {
		peer.Log.Debug(err)
		return
	}

	record := storage.Record{
		ID:                      peer.ID,
		IP:                      peer.Host.IP,
		Hostname:                peer.Host.Hostname,
		Tag:                     peer.Tag,
		Cost:                    req.Cost,
		PieceCount:              int32(peer.FinishedPieces.Count()),
		TotalPieceCount:         peer.Task.TotalPieceCount.Load(),
		ContentLength:           peer.Task.ContentLength.Load(),
		SecurityDomain:          peer.Host.SecurityDomain,
		IDC:                     peer.Host.IDC,
		NetTopology:             peer.Host.NetTopology,
		Location:                peer.Host.Location,
		FreeUploadCount:         peer.Host.FreeUploadCount(),
		State:                   peerState,
		HostType:                int(peer.Host.Type),
		CreateAt:                peer.CreateAt.Load().UnixNano(),
		UpdateAt:                peer.UpdateAt.Load().UnixNano(),
		ParentID:                parent.ID,
		ParentIP:                parent.Host.IP,
		ParentHostname:          parent.Host.Hostname,
		ParentTag:               parent.Tag,
		ParentPieceCount:        int32(parent.FinishedPieces.Count()),
		ParentSecurityDomain:    parent.Host.SecurityDomain,
		ParentIDC:               parent.Host.IDC,
		ParentNetTopology:       parent.Host.NetTopology,
		ParentLocation:          parent.Host.Location,
		ParentFreeUploadCount:   parent.Host.FreeUploadCount(),
		ParentUploadCount:       parent.Host.UploadCount.Load(),
		ParentUploadFailedCount: parent.Host.UploadFailedCount.Load(),
		ParentHostType:          int(parent.Host.Type),
		ParentCreateAt:          parent.CreateAt.Load().UnixNano(),
		ParentUpdateAt:          parent.UpdateAt.Load().UnixNano(),
	}

	if err := s.storage.Create(record); err != nil {
		peer.Log.Error(err)
	}
}
