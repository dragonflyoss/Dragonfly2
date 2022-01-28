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

	"d7y.io/dragonfly/v2/internal/dferrors"
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

// CDN is cdn resource
func (s *Service) CDN() resource.CDN {
	return s.resource.CDN()
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
	host := s.registerHost(ctx, req)
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
			// If the file is registered as a small type,
			// there is no need to build a tree, just find the parent and return
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
	peer.Log.Info("task is unsuccessful and needs to be register")
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

		peer.Log.Infof("receive piece: %#v %#v", piece, piece.PieceInfo)

		if piece.PieceInfo != nil {
			// Handle begin of piece
			if piece.PieceInfo.PieceNum == common.BeginOfPiece {
				s.handleBeginOfPiece(ctx, peer)
				continue
			}

			// Handle end of piece
			if piece.PieceInfo.PieceNum == common.EndOfPiece {
				s.handleEndOfPiece(ctx, peer)
				continue
			}
		}

		// Handle piece download successfully
		if piece.Success {
			s.handlePieceSuccess(ctx, peer, piece)

			// Collect peer host traffic metrics
			if s.config.Metrics != nil && s.config.Metrics.EnablePeerHost {
				metrics.PeerHostTraffic.WithLabelValues("download", peer.Host.ID, peer.Host.IP).Add(float64(piece.PieceInfo.RangeSize))
				if parent, ok := s.resource.PeerManager().Load(piece.DstPid); ok {
					metrics.PeerHostTraffic.WithLabelValues("upload", parent.Host.ID, parent.Host.IP).Add(float64(piece.PieceInfo.RangeSize))
				} else {
					peer.Log.Warnf("dst peer %s not found for piece %#v %#v", piece.DstPid, piece, piece.PieceInfo)
				}
			}
			continue
		}

		// Handle piece download code
		if piece.Code != base.Code_Success {
			// FIXME(244372610) When dfdaemon download peer return empty, retry later.
			if piece.Code == base.Code_ClientWaitPieceReady {
				peer.Log.Infof("receive piece code %d and wait for dfdaemon piece ready", piece.Code)
				continue
			}

			// Handle piece download failed
			peer.Log.Errorf("receive failed piece: %#v %#v", piece, piece.PieceInfo)
			s.handlePieceFail(ctx, peer, piece)
		}
	}
}

// ReportPeerResult handles peer result reported by dfdaemon
func (s *Service) ReportPeerResult(ctx context.Context, req *rpcscheduler.PeerResult) error {
	peer, ok := s.resource.PeerManager().Load(req.PeerId)
	if !ok {
		logger.Errorf("report peer result: peer %s is not exists", req.PeerId)
		return dferrors.Newf(base.Code_SchedPeerNotFound, "peer %s not found", req.PeerId)
	}

	peer.Log.Infof("report peer result request: %#v", req)
	if !req.Success {
		if peer.FSM.Is(resource.PeerStateBackToSource) {
			s.handleTaskFail(ctx, peer.Task)
		}
		s.handlePeerFail(ctx, peer)
		return nil
	}

	if peer.FSM.Is(resource.PeerStateBackToSource) {
		s.handleTaskSuccess(ctx, peer.Task, req)
	}
	s.handlePeerSuccess(ctx, peer)
	return nil
}

// LeaveTask makes the peer unschedulable
func (s *Service) LeaveTask(ctx context.Context, req *rpcscheduler.PeerTarget) error {
	peer, ok := s.resource.PeerManager().Load(req.PeerId)
	if !ok {
		logger.Errorf("leave task: peer %s is not exists", req.PeerId)
		return dferrors.Newf(base.Code_SchedPeerNotFound, "peer %s not found", req.PeerId)
	}

	peer.Log.Infof("leave task request: %#v", req)
	if err := peer.FSM.Event(resource.PeerEventLeave); err != nil {
		peer.Log.Errorf("peer fsm event failed: %v", err)
		return dferrors.Newf(base.Code_SchedTaskStatusError, err.Error())
	}

	peer.Children.Range(func(_, value interface{}) bool {
		child, ok := value.(*resource.Peer)
		if !ok {
			return true
		}

		// Reschedule a new parent to children of peer to exclude the current leave peer
		blocklist := set.NewSafeSet()
		blocklist.Add(peer.ID)

		s.scheduler.ScheduleParent(ctx, child, blocklist)
		return true
	})

	peer.DeleteParent()
	s.resource.PeerManager().Delete(peer.ID)
	return nil
}

// registerTask creates a new task or reuses a previous task
func (s *Service) registerTask(ctx context.Context, req *rpcscheduler.PeerTaskRequest) (*resource.Task, error) {
	task := resource.NewTask(idgen.TaskID(req.Url, req.UrlMeta), req.Url, s.config.Scheduler.BackSourceCount, req.UrlMeta)
	task, loaded := s.resource.TaskManager().LoadOrStore(task)
	if loaded && (task.FSM.Is(resource.TaskStateRunning) || task.FSM.Is(resource.TaskStateSucceeded)) && task.LenAvailablePeers() != 0 {
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
			s.handleTaskFail(ctx, task)
			return
		}

		// Update the task status first to help peer scheduling evaluation and scoring
		s.handleTaskSuccess(ctx, task, endOfPiece)
		s.handlePeerSuccess(ctx, peer)
	}()

	return task, nil
}

// registerHost creates a new host or reuses a previous host
func (s *Service) registerHost(ctx context.Context, req *rpcscheduler.PeerTaskRequest) *resource.Host {
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

// registerPeer creates a new peer or reuses a previous peer
func (s *Service) registerPeer(ctx context.Context, req *rpcscheduler.PeerTaskRequest, task *resource.Task, host *resource.Host) *resource.Peer {
	peer, loaded := s.resource.PeerManager().LoadOrStore(resource.NewPeer(req.PeerId, task, host))
	if !loaded {
		peer.Log.Info("create new peer")
	} else {
		peer.Log.Info("peer already exists")
	}

	return peer
}

// handleBeginOfPiece handles begin of piece
func (s *Service) handleBeginOfPiece(ctx context.Context, peer *resource.Peer) {
	switch peer.FSM.Current() {
	case resource.PeerStateBackToSource:
		// Back to the source download process, peer directly returns
		peer.Log.Info("peer back to source")
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

		// It’s not a case of back-to-source or small task downloading,
		// to help peer to schedule the parent node
		blocklist := set.NewSafeSet()
		blocklist.Add(peer.ID)
		s.scheduler.ScheduleParent(ctx, peer, blocklist)
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
	peer.AppendPieceCost(int64(piece.EndTime - piece.BeginTime))

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
		peer.Log.Error("peer back to source finished with fail piece")
		return
	}

	// If parent can not found, reschedule parent.
	parent, ok := s.resource.PeerManager().Load(piece.DstPid)
	if !ok {
		peer.Log.Errorf("can not found parent %s and reschedule", piece.DstPid)
		s.scheduler.ScheduleParent(ctx, peer, set.NewSafeSet())
		return
	}

	// It’s not a case of back-to-source downloading failed,
	// to help peer to reschedule the parent node
	switch piece.Code {
	case base.Code_ClientPieceDownloadFail, base.Code_PeerTaskNotFound, base.Code_CDNError, base.Code_CDNTaskDownloadFail:
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
		go func() {
			parent.Log.Info("cdn restart seed task")
			cdnPeer, endOfPiece, err := s.resource.CDN().TriggerTask(context.Background(), parent.Task)
			if err != nil {
				peer.Log.Errorf("retrigger task failed: %v", err)
				s.handleTaskFail(ctx, parent.Task)
				return
			}

			s.handleTaskSuccess(ctx, cdnPeer.Task, endOfPiece)
			s.handlePeerSuccess(ctx, cdnPeer)
		}()
	default:
	}

	// Peer state is PeerStateRunning will be rescheduled
	if !peer.FSM.Is(resource.PeerStateRunning) {
		peer.Log.Infof("peer can not be rescheduled because peer state is %s", peer.FSM.Current())
		return
	}

	blocklist := set.NewSafeSet()
	blocklist.Add(parent.ID)

	s.scheduler.ScheduleParent(ctx, peer, blocklist)
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
			peer.Log.Warnf("download tiny file length is %d, task content length is %d, download is failed: %v", len(data), peer.Task.ContentLength.Load(), err)
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
	blocklist := set.NewSafeSet()
	blocklist.Add(peer.ID)

	peer.Children.Range(func(_, value interface{}) bool {
		child, ok := value.(*resource.Peer)
		if !ok {
			return true
		}

		s.scheduler.ScheduleParent(ctx, child, blocklist)
		return true
	})
}

// Conditions for the task to switch to the TaskStateSucceeded are:
// 1. CDN downloads the resource successfully
// 2. Dfdaemon back-to-source to download successfully
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
	if task.FSM.Is(resource.TaskStateFailed) {
		return
	}

	if err := task.FSM.Event(resource.TaskEventDownloadFailed); err != nil {
		task.Log.Errorf("task fsm event failed: %v", err)
		return
	}
}
