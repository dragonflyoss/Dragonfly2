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

	"go.opentelemetry.io/otel/trace"

	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/container/set"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/base/common"
	rpcscheduler "d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/pkg/util/timeutils"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/metrics"
	"d7y.io/dragonfly/v2/scheduler/resource"
	"d7y.io/dragonfly/v2/scheduler/scheduler"
)

type Service struct {
	// Resource interface
	resource resource.Resource

	// Scheduler interface
	scheduler scheduler.Scheduler

	// Scheduelr service config
	config *config.Config

	// Dynamic config
	dynconfig config.DynconfigInterface
}

// New service instance
func New(
	cfg *config.Config,
	resource resource.Resource,
	scheduler scheduler.Scheduler,
	dynconfig config.DynconfigInterface,
) *Service {

	return &Service{
		resource:  resource,
		scheduler: scheduler,
		config:    cfg,
		dynconfig: dynconfig,
	}
}

// RegisterPeerTask registers peer and triggers CDN download task
func (s *Service) RegisterPeerTask(ctx context.Context, req *rpcscheduler.PeerTaskRequest) (*rpcscheduler.RegisterResult, error) {
	// Register task and trigger cdn download task
	task, err := s.registerTask(ctx, req)
	if err != nil {
		dferr := dferrors.New(base.Code_SchedTaskStatusError, "register task is fail")
		logger.Errorf("peer %s register is failed: %v", req.PeerId, err)
		return nil, dferr
	}
	host := s.registerHost(ctx, req.PeerHost)
	peer := s.registerPeer(ctx, req.PeerId, task, host, req.UrlMeta.Tag)
	peer.Log.Infof("register peer task request: %#v %#v %#v", req, req.UrlMeta, req.HostLoad)

	// Task has been successful
	if task.FSM.Is(resource.TaskStateSucceeded) {
		peer.Log.Info("tasks can be reused")
		sizeScope := task.SizeScope()
		switch sizeScope {
		case base.SizeScope_TINY:
			peer.Log.Info("task size scope is tiny and return piece content directly")
			if len(task.DirectPiece) > 0 && int64(len(task.DirectPiece)) == task.ContentLength.Load() {
				if err := peer.FSM.Event(resource.PeerEventRegisterTiny); err != nil {
					dferr := dferrors.New(base.Code_SchedError, err.Error())
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
			// There is no need to build a tree, just find the parent and return
			parent, ok := s.scheduler.FindParent(ctx, peer, set.NewSafeSet())
			if !ok {
				peer.Log.Warn("task size scope is small and it can not select parent")
				if err := peer.FSM.Event(resource.PeerEventRegisterNormal); err != nil {
					dferr := dferrors.New(base.Code_SchedError, err.Error())
					peer.Log.Errorf("peer %s register is failed: %v", req.PeerId, err)
					return nil, dferr
				}

				return &rpcscheduler.RegisterResult{
					TaskId:    task.ID,
					SizeScope: base.SizeScope_NORMAL,
				}, nil
			}

			// When task size scope is small, parent must be downloaded successfully
			// before returning to the parent directly
			if !parent.FSM.Is(resource.PeerStateSucceeded) {
				peer.Log.Infof("task size scope is small and download state %s is not PeerStateSucceeded", parent.FSM.Current())
				if err := peer.FSM.Event(resource.PeerEventRegisterNormal); err != nil {
					dferr := dferrors.New(base.Code_SchedError, err.Error())
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
					dferr := dferrors.New(base.Code_SchedError, err.Error())
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
				dferr := dferrors.New(base.Code_SchedError, err.Error())
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
				dferr := dferrors.New(base.Code_SchedError, err.Error())
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
	peer.Log.Infof("task state is %s and needs to be register", task.FSM.Current())
	if err := peer.FSM.Event(resource.PeerEventRegisterNormal); err != nil {
		dferr := dferrors.New(base.Code_SchedError, err.Error())
		peer.Log.Errorf("peer %s register is failed: %v", req.PeerId, err)
		return nil, dferr
	}

	return &rpcscheduler.RegisterResult{
		TaskId:    task.ID,
		SizeScope: base.SizeScope_NORMAL,
	}, nil
}

// ReportPieceResult handles the piece information reported by dfdaemon
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
			logger.Errorf("receive piece %#v error: %v", piece, err)
			return err
		}

		if !initialized {
			initialized = true

			// Get peer from peer manager
			peer, ok = s.resource.PeerManager().Load(piece.SrcPid)
			if !ok {
				dferr := dferrors.Newf(base.Code_SchedPeerNotFound, "peer %s not found", piece.SrcPid)
				logger.Errorf("peer %s not found", piece.SrcPid)
				return dferr
			}

			// Peer setting stream
			peer.StoreStream(stream)
			defer peer.DeleteStream()
		}

		if piece.PieceInfo != nil {
			// Handle begin of piece
			if piece.PieceInfo.PieceNum == common.BeginOfPiece {
				peer.Log.Infof("receive begin of piece: %#v %#v", piece, piece.PieceInfo)
				s.handleBeginOfPiece(ctx, peer)
				continue
			}

			// Handle end of piece
			if piece.PieceInfo.PieceNum == common.EndOfPiece {
				peer.Log.Infof("receive end of piece: %#v %#v", piece, piece.PieceInfo)
				s.handleEndOfPiece(ctx, peer)
				continue
			}
		}

		// Handle piece download successfully
		if piece.Success {
			peer.Log.Infof("receive piece: %#v %#v", piece, piece.PieceInfo)
			s.handlePieceSuccess(ctx, peer, piece)

			// Collect peer host traffic metrics
			if s.config.Metrics != nil && s.config.Metrics.EnablePeerHost {
				metrics.PeerHostTraffic.WithLabelValues(peer.BizTag, metrics.PeerHostTrafficDownloadType, peer.Host.ID, peer.Host.IP).Add(float64(piece.PieceInfo.RangeSize))
				if parent, ok := s.resource.PeerManager().Load(piece.DstPid); ok {
					metrics.PeerHostTraffic.WithLabelValues(peer.BizTag, metrics.PeerHostTrafficUploadType, parent.Host.ID, parent.Host.IP).Add(float64(piece.PieceInfo.RangeSize))
				} else {
					peer.Log.Warnf("dst peer %s not found for piece %#v %#v", piece.DstPid, piece, piece.PieceInfo)
				}
			}

			// Collect traffic metrics
			if piece.DstPid != "" {
				metrics.Traffic.WithLabelValues(peer.BizTag, metrics.TrafficP2PType).Add(float64(piece.PieceInfo.RangeSize))
			} else {
				metrics.Traffic.WithLabelValues(peer.BizTag, metrics.TrafficBackToSourceType).Add(float64(piece.PieceInfo.RangeSize))
			}
			continue
		}

		// Handle piece download code
		if piece.Code != base.Code_Success {
			if piece.Code == base.Code_ClientWaitPieceReady {
				peer.Log.Debugf("receive piece code %d and wait for dfdaemon piece ready", piece.Code)
				continue
			}

			// Handle piece download failed
			peer.Log.Errorf("receive failed piece: %#v", piece)
			s.handlePieceFail(ctx, peer, piece)
			continue
		}

		peer.Log.Warnf("receive unknow piece: %#v %#v", piece, piece.PieceInfo)
	}
}

// ReportPeerResult handles peer result reported by dfdaemon
func (s *Service) ReportPeerResult(ctx context.Context, req *rpcscheduler.PeerResult) error {
	peer, ok := s.resource.PeerManager().Load(req.PeerId)
	if !ok {
		logger.Errorf("report peer result and peer %s is not exists", req.PeerId)
		return dferrors.Newf(base.Code_SchedPeerNotFound, "peer %s not found", req.PeerId)
	}

	metrics.DownloadCount.WithLabelValues(peer.BizTag).Inc()

	if !req.Success {
		peer.Log.Errorf("report peer failed result: %s %#v", req.Code, req)
		if peer.FSM.Is(resource.PeerStateBackToSource) {
			s.handleTaskFail(ctx, peer.Task)
			metrics.DownloadFailureCount.WithLabelValues(peer.BizTag, metrics.DownloadFailureBackToSourceType).Inc()
		} else {
			metrics.DownloadFailureCount.WithLabelValues(peer.BizTag, metrics.DownloadFailureP2PType).Inc()
		}

		s.handlePeerFail(ctx, peer)
		return nil
	}

	peer.Log.Infof("report peer result: %#v", req)
	if peer.FSM.Is(resource.PeerStateBackToSource) {
		s.handleTaskSuccess(ctx, peer.Task, req)
	}
	s.handlePeerSuccess(ctx, peer)

	metrics.PeerTaskDownloadDuration.WithLabelValues(peer.BizTag).Observe(float64(req.Cost))
	return nil
}

// LeaveTask makes the peer unschedulable
func (s *Service) LeaveTask(ctx context.Context, req *rpcscheduler.PeerTarget) error {
	peer, ok := s.resource.PeerManager().Load(req.PeerId)
	if !ok {
		logger.Errorf("leave task and peer %s is not exists", req.PeerId)
		return dferrors.Newf(base.Code_SchedPeerNotFound, "peer %s not found", req.PeerId)
	}

	metrics.LeaveTaskCount.WithLabelValues(peer.BizTag).Inc()

	peer.Log.Infof("leave task: %#v", req)
	if err := peer.FSM.Event(resource.PeerEventLeave); err != nil {
		peer.Log.Errorf("peer fsm event failed: %v", err)

		metrics.LeaveTaskFailureCount.WithLabelValues(peer.BizTag).Inc()
		return dferrors.Newf(base.Code_SchedTaskStatusError, err.Error())
	}

	peer.Children.Range(func(_, value interface{}) bool {
		child, ok := value.(*resource.Peer)
		if !ok {
			return true
		}

		// Reschedule a new parent to children of peer to exclude the current leave peer
		child.Log.Infof("schedule parent because of parent peer %s is leaving", peer.ID)
		s.scheduler.ScheduleParent(ctx, child, child.BlockPeers)
		return true
	})

	s.resource.PeerManager().Delete(peer.ID)
	return nil
}

// StatPeerTask checks if the given task exists in P2P network
func (s *Service) StatPeerTask(ctx context.Context, req *rpcscheduler.StatPeerTaskRequest) (*base.GrpcDfResult, error) {
	log := logger.With("function", "StatPeerTask", "TaskID", req.TaskId)

	// Check if task exists
	task, loaded := s.resource.TaskManager().Load(req.TaskId)
	if !loaded || task == nil {
		msg := "task not found in P2P network"
		log.Info(msg)
		return common.NewGrpcDfResult(base.Code_PeerTaskNotFound, msg), nil
	}
	if task.FSM.Current() != resource.TaskStateSucceeded {
		msg := fmt.Sprintf("task found but not in %s state: %s", resource.TaskStateSucceeded, task.FSM.Current())
		log.Info(msg)
		return common.NewGrpcDfResult(base.Code_PeerTaskNotFound, msg), nil
	}

	lenPeers := task.PeerCount.Load()
	if lenPeers <= 0 {
		msg := fmt.Sprintf("task found but with %d active peers", lenPeers)
		log.Info(msg)
		return common.NewGrpcDfResult(base.Code_PeerTaskNotFound, msg), nil
	}

	msg := "task found in P2P network"
	log.Info(msg)
	return common.NewGrpcDfResult(base.Code_Success, msg), nil
}

// AnnounceTask informs scheduler a peer has completed task
func (s *Service) AnnounceTask(ctx context.Context, req *rpcscheduler.AnnounceTaskRequest) (*base.GrpcDfResult, error) {
	log := logger.With("function", "AnnounceTask", "TaskID", req.TaskId, "CID", req.Cid)

	// Must contain valid PiecePacket.PieceInfos
	if req.PiecePacket == nil || req.PiecePacket.PieceInfos == nil {
		msg := "no piece info found in request"
		log.Warn(msg)
		return common.NewGrpcDfResult(base.Code_BadRequest, msg), nil
	}

	pieceInfos := req.PiecePacket.PieceInfos
	totalPiece := req.PiecePacket.TotalPiece
	contentLength := req.PiecePacket.ContentLength
	peerID := req.PiecePacket.DstPid
	if totalPiece != int32(len(pieceInfos)) {
		msg := fmt.Sprintf("total piece count mismatch in request: %d != %d", totalPiece, len(pieceInfos))
		log.Warn(msg)
		return common.NewGrpcDfResult(base.Code_BadRequest, msg), nil
	}

	// Check if taskID is valid
	if req.TaskId != req.PiecePacket.TaskId {
		msg := fmt.Sprintf("task ID mismatch %s != %s", req.TaskId, req.PiecePacket.TaskId)
		log.Warn(msg)
		return common.NewGrpcDfResult(base.Code_BadRequest, msg), nil
	}

	task := resource.NewTask(req.TaskId, req.Cid, 0, req.UrlMeta)
	task, _ = s.resource.TaskManager().LoadOrStore(task)

	host := s.registerHost(ctx, req.PeerHost)
	peer := s.registerPeer(ctx, peerID, task, host, "")
	peer.Log.Infof("announce peer task request: %#v", req)

	// Update task piece infos, and mark task as Success
	if !task.FSM.Is(resource.TaskStateSucceeded) {
		if task.FSM.Is(resource.TaskStatePending) {
			if err := task.FSM.Event(resource.TaskEventDownload); err != nil {
				return nil, err
			}
		}
		for _, info := range pieceInfos {
			task.StorePiece(info)
			peer.Pieces.Set(uint(info.PieceNum))
		}
		peerResult := rpcscheduler.PeerResult{
			TaskId:          req.TaskId,
			PeerId:          peerID,
			SrcIp:           req.PeerHost.Ip,
			Url:             req.Cid,
			Success:         true,
			TotalPieceCount: totalPiece,
			ContentLength:   contentLength,
			Code:            base.Code_Success,
		}
		s.handleTaskSuccess(ctx, task, &peerResult)
	}

	// Trigger the peer state machine, so we could change peer state to success as well
	if !peer.FSM.Is(resource.PeerStateSucceeded) {
		if peer.FSM.Is(resource.PeerStatePending) {
			if err := peer.FSM.Event(resource.PeerEventRegisterNormal); err != nil {
				peer.Log.Errorf("peer fsm event failed: %s", err)
			}
		}
		if peer.FSM.Is(resource.PeerStateReceivedTiny) ||
			peer.FSM.Is(resource.PeerStateReceivedSmall) ||
			peer.FSM.Is(resource.PeerStateReceivedNormal) {
			if err := peer.FSM.Event(resource.PeerEventDownload); err != nil {
				peer.Log.Errorf("peer fsm event failed: %s", err)
			} else {
				s.handlePeerSuccess(ctx, peer)
			}
		}
	}

	return common.NewGrpcDfResult(base.Code_Success, "announce task succeeded"), nil
}

// registerTask creates a new task or reuses a previous task
func (s *Service) registerTask(ctx context.Context, req *rpcscheduler.PeerTaskRequest) (*resource.Task, error) {
	task := resource.NewTask(idgen.TaskID(req.Url, req.UrlMeta), req.Url, s.config.Scheduler.BackSourceCount, req.UrlMeta)
	task, loaded := s.resource.TaskManager().LoadOrStore(task)
	if loaded && task.HasAvailablePeer() && (task.FSM.Is(resource.TaskStateSucceeded) || task.FSM.Is(resource.TaskStateRunning)) {
		task.Log.Infof("task state is %s and it has available peer", task.FSM.Current())
		return task, nil
	}

	// Trigger task
	if err := task.FSM.Event(resource.TaskEventDownload); err != nil {
		return nil, err
	}

	// Start trigger cdn task
	if s.config.CDN.Enable {
		go s.triggerCDNTask(ctx, task)
	}

	return task, nil
}

// registerHost creates a new host or reuses a previous host
func (s *Service) registerHost(ctx context.Context, rawHost *rpcscheduler.PeerHost) *resource.Host {
	host, ok := s.resource.HostManager().Load(rawHost.Uuid)
	if !ok {
		// Get scheduler cluster client config by manager
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

// registerPeer creates a new peer or reuses a previous peer
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

// triggerCDNTask starts trigger cdn task
func (s *Service) triggerCDNTask(ctx context.Context, task *resource.Task) {
	task.Log.Infof("trigger cdn download task and task status is %s", task.FSM.Current())
	peer, endOfPiece, err := s.resource.CDN().TriggerTask(
		trace.ContextWithSpanContext(context.Background(), trace.SpanContextFromContext(ctx)), task)
	if err != nil {
		task.Log.Errorf("trigger cdn download task failed: %v", err)
		s.handleTaskFail(ctx, task)
		return
	}

	// Update the task status first to help peer scheduling evaluation and scoring
	peer.Log.Info("trigger cdn download task successfully")
	s.handleTaskSuccess(ctx, task, endOfPiece)
	s.handlePeerSuccess(ctx, peer)
}

// handleBeginOfPiece handles begin of piece
func (s *Service) handleBeginOfPiece(ctx context.Context, peer *resource.Peer) {
	switch peer.FSM.Current() {
	case resource.PeerStateBackToSource:
		// Back to the source download process, peer directly returns
		peer.Log.Info("peer downloads back-to-source when receive the begin of piece")
		return
	case resource.PeerStateReceivedTiny:
		// When the task is tiny,
		// the peer has already returned to piece data when registering
		peer.Log.Info("file type is tiny, peer has already returned to piece data when registering")
		if err := peer.FSM.Event(resource.PeerEventDownload); err != nil {
			peer.Log.Errorf("peer fsm event failed: %v", err)
			return
		}
	case resource.PeerStateReceivedSmall:
		// When the task is small,
		// the peer has already returned to the parent when registering
		peer.Log.Info("file type is small, peer has already returned to the parent when registering")
		if err := peer.FSM.Event(resource.PeerEventDownload); err != nil {
			peer.Log.Errorf("peer fsm event failed: %v", err)
			return
		}
	case resource.PeerStateReceivedNormal:
		if err := peer.FSM.Event(resource.PeerEventDownload); err != nil {
			peer.Log.Errorf("peer fsm event failed: %v", err)
			return
		}

		peer.Log.Infof("schedule parent because of peer receive begin of piece")
		s.scheduler.ScheduleParent(ctx, peer, set.NewSafeSet())
	default:
		peer.Log.Warnf("peer state is %s when receive the begin of piece", peer.FSM.Current())
	}
}

// handleEndOfPiece handles end of piece
func (s *Service) handleEndOfPiece(ctx context.Context, peer *resource.Peer) {}

// handlePieceSuccess handles successful piece
func (s *Service) handlePieceSuccess(ctx context.Context, peer *resource.Peer, piece *rpcscheduler.PieceResult) {
	// Update peer piece info
	peer.Pieces.Set(uint(piece.PieceInfo.PieceNum))
	peer.AppendPieceCost(timeutils.SubNano(int64(piece.EndTime), int64(piece.BeginTime)).Milliseconds())

	// When the peer downloads back-to-source,
	// piece downloads successfully updates the task piece info
	if peer.FSM.Is(resource.PeerStateBackToSource) {
		peer.Task.StorePiece(piece.PieceInfo)
	}
}

// handlePieceFail handles failed piece
func (s *Service) handlePieceFail(ctx context.Context, peer *resource.Peer, piece *rpcscheduler.PieceResult) {
	// Failed to download piece back-to-source
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
	// to help peer to reschedule the parent node
	switch piece.Code {
	case base.Code_PeerTaskNotFound, base.Code_CDNError, base.Code_CDNTaskDownloadFail:
		if err := parent.FSM.Event(resource.PeerEventDownloadFailed); err != nil {
			peer.Log.Errorf("peer fsm event failed: %v", err)
			break
		}
	case base.Code_ClientPieceNotFound:
		// Dfdaemon downloading piece data from parent returns http error code 404.
		// If the parent is not a CDN, reschedule parent for peer.
		// If the parent is a CDN, scheduler need to trigger CDN to download again.
		if !parent.Host.IsCDN {
			peer.Log.Infof("parent %s is not cdn", piece.DstPid)
			break
		}

		peer.Log.Infof("parent %s is cdn", piece.DstPid)
		fallthrough
	case base.Code_CDNTaskNotFound:
		s.handlePeerFail(ctx, parent)

		// Start trigger cdn task
		if s.config.CDN.Enable {
			go s.triggerCDNTask(ctx, parent.Task)
		}
	default:
	}

	// Peer state is PeerStateRunning will be rescheduled
	if !peer.FSM.Is(resource.PeerStateRunning) {
		peer.Log.Infof("peer can not be rescheduled because peer state is %s", peer.FSM.Current())
		return
	}

	peer.Log.Infof("schedule parent because of peer receive failed piece")
	peer.BlockPeers.Add(parent.ID)
	s.scheduler.ScheduleParent(ctx, peer, peer.BlockPeers)
}

// handlePeerSuccess handles successful peer
func (s *Service) handlePeerSuccess(ctx context.Context, peer *resource.Peer) {
	// If the peer type is tiny and back-to-source,
	// it need to directly download the tiny file and store the data in task DirectPiece
	if peer.Task.SizeScope() == base.SizeScope_TINY && len(peer.Task.DirectPiece) == 0 {
		data, err := peer.DownloadTinyFile()
		if err == nil && len(data) == int(peer.Task.ContentLength.Load()) {
			// Tiny file downloaded successfully
			peer.Task.DirectPiece = data
		} else {
			peer.Log.Warnf("download tiny file length is %d, task content length is %d, downloading is failed: %v", len(data), peer.Task.ContentLength.Load(), err)
		}
	}

	if err := peer.FSM.Event(resource.PeerEventDownloadSucceeded); err != nil {
		peer.Log.Errorf("peer fsm event failed: %v", err)
		return
	}
}

// handlePeerFail handles failed peer
func (s *Service) handlePeerFail(ctx context.Context, peer *resource.Peer) {
	if err := peer.FSM.Event(resource.PeerEventDownloadFailed); err != nil {
		peer.Log.Errorf("peer fsm event failed: %v", err)
		return
	}

	// Reschedule a new parent to children of peer to exclude the current failed peer
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
// 1. CDN downloads the resource successfully
// 2. Dfdaemon back-to-source to download successfully
// 3. Peer announces it has the task
func (s *Service) handleTaskSuccess(ctx context.Context, task *resource.Task, result *rpcscheduler.PeerResult) {
	if task.FSM.Is(resource.TaskStateSucceeded) {
		return
	}

	if err := task.FSM.Event(resource.TaskEventDownloadSucceeded); err != nil {
		task.Log.Errorf("task fsm event failed: %v", err)
		return
	}

	// Update task's resource total piece count and content length
	task.TotalPieceCount.Store(result.TotalPieceCount)
	task.ContentLength.Store(result.ContentLength)
}

// Conditions for the task to switch to the TaskStateSucceeded are:
// 1. CDN downloads the resource falied
// 2. Dfdaemon back-to-source to download failed
func (s *Service) handleTaskFail(ctx context.Context, task *resource.Task) {
	// If the number of failed peers in the task is greater than FailedPeerCountLimit,
	// then scheduler notifies running peers of failure
	if task.PeerFailedCount.Load() > resource.FailedPeerCountLimit {
		task.PeerFailedCount.Store(0)
		task.NotifyPeers(base.Code_SchedTaskStatusError, resource.PeerEventDownloadFailed)
	}

	if task.FSM.Is(resource.TaskStateFailed) {
		return
	}

	if err := task.FSM.Event(resource.TaskEventDownloadFailed); err != nil {
		task.Log.Errorf("task fsm event failed: %v", err)
		return
	}
}
