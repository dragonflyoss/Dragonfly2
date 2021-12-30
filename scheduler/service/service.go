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
	"time"

	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/base/common"
	rpcscheduler "d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	pkgsync "d7y.io/dragonfly/v2/pkg/sync"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/entity"
	"d7y.io/dragonfly/v2/scheduler/manager"
	"d7y.io/dragonfly/v2/scheduler/metrics"
	"d7y.io/dragonfly/v2/scheduler/scheduler"
)

type Service interface {
	Scheduler() scheduler.Scheduler
	Manager() *manager.Manager
	RegisterTask(context.Context, *rpcscheduler.PeerTaskRequest) (*entity.Task, error)
	LoadOrStoreHost(context.Context, *rpcscheduler.PeerTaskRequest) *entity.Host
	LoadOrStorePeer(context.Context, *rpcscheduler.PeerTaskRequest, *entity.Task, *entity.Host) *entity.Peer
	LoadPeer(string) (*entity.Peer, bool)
	HandlePiece(context.Context, *entity.Peer, *rpcscheduler.PieceResult)
	HandlePeer(context.Context, *entity.Peer, *rpcscheduler.PeerResult)
	HandlePeerLeave(ctx context.Context, peer *entity.Peer)
}

type service struct {
	// Manager entity instance
	manager *manager.Manager

	// Scheduler instance
	scheduler scheduler.Scheduler

	// callback holds some actions, like task fail, piece success actions
	callback Callback

	// Scheduelr service config
	config *config.Config

	// Dynamic config
	dynconfig config.DynconfigInterface

	// Key map mutex
	kmu *pkgsync.Krwmutex
}

func New(cfg *config.Config, scheduler scheduler.Scheduler, manager *manager.Manager, dynconfig config.DynconfigInterface) (Service, error) {
	// Initialize callback
	callback := newCallback(cfg, manager, scheduler)

	return &service{
		manager:   manager,
		scheduler: scheduler,
		callback:  callback,
		config:    cfg,
		dynconfig: dynconfig,
		kmu:       pkgsync.NewKrwmutex(),
	}, nil
}

func (s *service) Scheduler() scheduler.Scheduler {
	return s.scheduler
}

func (s *service) Manager() *manager.Manager {
	return s.manager
}

func (s *service) RegisterTask(ctx context.Context, req *rpcscheduler.PeerTaskRequest) (*entity.Task, error) {
	task := entity.NewTask(idgen.TaskID(req.Url, req.UrlMeta), req.Url, s.config.Scheduler.BackSourceCount, req.UrlMeta)

	s.kmu.Lock(task.ID)
	defer s.kmu.Unlock(task.ID)
	task, ok := s.manager.Task.LoadOrStore(task)
	if ok && (task.FSM.Is(entity.TaskStateRunning) || task.FSM.Is(entity.TaskStateSucceeded)) {
		// Task is healthy and can be reused
		task.UpdateAt.Store(time.Now())
		task.Log.Infof("reuse task and status is %s", task.FSM.Current())
		return task, nil
	}

	// Trigger task
	if err := task.FSM.Event(entity.TaskEventDownload); err != nil {
		return nil, err
	}

	// Start seed cdn task
	go func() {
		task.Log.Info("cdn start seed task")
		peer, endOfPiece, err := s.manager.CDN.TriggerTask(context.Background(), task)
		if err != nil {
			task.Log.Errorf("trigger task failed: %v", err)

			// Update the peer status first to help task return the error code to the peer that is downloading
			// If init cdn fails, peer is nil
			s.callback.TaskFail(ctx, task)
			if peer != nil {
				s.callback.PeerFail(ctx, peer)
			}
			return
		}

		// Update the task status first to help peer scheduling evaluation and scoring
		s.callback.TaskSuccess(ctx, peer, task, endOfPiece)
		s.callback.PeerSuccess(ctx, peer)
	}()

	return task, nil
}

func (s *service) LoadOrStoreHost(ctx context.Context, req *rpcscheduler.PeerTaskRequest) *entity.Host {
	rawHost := req.PeerHost
	host, ok := s.manager.Host.Load(rawHost.Uuid)
	if !ok {
		// Get scheduler cluster client config by manager
		var options []entity.HostOption
		if clientConfig, ok := s.dynconfig.GetSchedulerClusterClientConfig(); ok {
			options = append(options, entity.WithUploadLoadLimit(int32(clientConfig.LoadLimit)))
		}

		host = entity.NewHost(rawHost, options...)
		s.manager.Host.Store(host)
		host.Log.Info("create host")
	}

	host.Log.Info("host already exists")
	return host
}

func (s *service) LoadOrStorePeer(ctx context.Context, req *rpcscheduler.PeerTaskRequest, task *entity.Task, host *entity.Host) *entity.Peer {
	peer := entity.NewPeer(req.PeerId, task, host)
	peer, _ = s.manager.Peer.LoadOrStore(peer)
	return peer
}

func (s *service) LoadPeer(id string) (*entity.Peer, bool) {
	return s.manager.Peer.Load(id)
}

func (s *service) HandlePiece(ctx context.Context, peer *entity.Peer, piece *rpcscheduler.PieceResult) {
	// Handle piece download successfully
	if piece.Success {
		s.callback.PieceSuccess(ctx, peer, piece)

		// Collect peer host traffic metrics
		if s.config.Metrics != nil && s.config.Metrics.EnablePeerHost {
			metrics.PeerHostTraffic.WithLabelValues("download", peer.Host.ID, peer.Host.IP).Add(float64(piece.PieceInfo.RangeSize))
			if p, ok := s.manager.Peer.Load(piece.DstPid); ok {
				metrics.PeerHostTraffic.WithLabelValues("upload", p.Host.ID, p.Host.IP).Add(float64(piece.PieceInfo.RangeSize))
			} else {
				peer.Log.Warnf("dst peer %s not found for piece %#v, pieceInfo %#v", piece.DstPid, piece, piece.PieceInfo)
			}
		}
		return
	}

	// Handle begin of piece and end of piece
	if piece.PieceInfo != nil {
		if piece.PieceInfo.PieceNum == common.BeginOfPiece {
			peer.Log.Infof("receive begin of piece: %#v", piece)
			s.callback.BeginOfPiece(ctx, peer)
			return
		}

		if piece.PieceInfo.PieceNum == common.EndOfPiece {
			peer.Log.Infof("receive end of piece: %#v", piece)
			s.callback.EndOfPiece(ctx, peer)
			return
		}
	}

	// Handle piece download failed
	if piece.Code != base.Code_Success {
		peer.Log.Infof("receive piece error: %v", piece.Code)
		s.callback.PieceFail(ctx, peer, piece)
		return
	}
}

func (s *service) HandlePeer(ctx context.Context, peer *entity.Peer, req *rpcscheduler.PeerResult) {
	if !req.Success {
		if peer.Task.BackToSourcePeers.Contains(peer) {
			s.callback.TaskFail(ctx, peer.Task)
		}
		s.callback.PeerFail(ctx, peer)
	}

	if peer.Task.BackToSourcePeers.Contains(peer) {
		s.callback.TaskSuccess(ctx, peer, peer.Task, req)
	}
	s.callback.PeerSuccess(ctx, peer)
}

func (s *service) HandlePeerLeave(ctx context.Context, peer *entity.Peer) {
	s.callback.PeerLeave(ctx, peer)
}
