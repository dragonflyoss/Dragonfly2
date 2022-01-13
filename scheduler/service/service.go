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
	"d7y.io/dragonfly/v2/scheduler/metrics"
	"d7y.io/dragonfly/v2/scheduler/resource"
	"d7y.io/dragonfly/v2/scheduler/scheduler"
)

type Service interface {
	Scheduler() scheduler.Scheduler
	CDN() resource.CDN
	RegisterTask(context.Context, *rpcscheduler.PeerTaskRequest) (*resource.Task, error)
	LoadOrStoreHost(context.Context, *rpcscheduler.PeerTaskRequest) (*resource.Host, bool)
	LoadOrStorePeer(context.Context, *rpcscheduler.PeerTaskRequest, *resource.Task, *resource.Host) (*resource.Peer, bool)
	LoadPeer(string) (*resource.Peer, bool)
	HandlePiece(context.Context, *resource.Peer, *rpcscheduler.PieceResult)
	HandlePeer(context.Context, *resource.Peer, *rpcscheduler.PeerResult)
	HandlePeerLeave(ctx context.Context, peer *resource.Peer)
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

	// Key map mutex
	kmu *pkgsync.Krwmutex
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
		kmu:       pkgsync.NewKrwmutex(),
	}
}

func (s *service) Scheduler() scheduler.Scheduler {
	return s.scheduler
}

func (s *service) CDN() resource.CDN {
	return s.resource.CDN()
}

func (s *service) RegisterTask(ctx context.Context, req *rpcscheduler.PeerTaskRequest) (*resource.Task, error) {
	task := resource.NewTask(idgen.TaskID(req.Url, req.UrlMeta), req.Url, s.config.Scheduler.BackSourceCount, req.UrlMeta)

	s.kmu.Lock(task.ID)
	defer s.kmu.Unlock(task.ID)
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
		task.Log.Info("cdn start seed task")
		peer, endOfPiece, err := s.resource.CDN().TriggerTask(context.Background(), task)
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
		s.callback.TaskSuccess(ctx, task, endOfPiece)
		s.callback.PeerSuccess(ctx, peer)
	}()

	return task, nil
}

func (s *service) LoadOrStoreHost(ctx context.Context, req *rpcscheduler.PeerTaskRequest) (*resource.Host, bool) {
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
		host.Log.Info("create host")
		return host, false
	}

	host.Log.Info("host already exists")
	return host, true
}

func (s *service) LoadOrStorePeer(ctx context.Context, req *rpcscheduler.PeerTaskRequest, task *resource.Task, host *resource.Host) (*resource.Peer, bool) {
	peer := resource.NewPeer(req.PeerId, task, host)
	return s.resource.PeerManager().LoadOrStore(peer)
}

func (s *service) LoadPeer(id string) (*resource.Peer, bool) {
	return s.resource.PeerManager().Load(id)
}

func (s *service) HandlePiece(ctx context.Context, peer *resource.Peer, piece *rpcscheduler.PieceResult) {
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

	// Handle piece download failed
	if piece.Code != base.Code_Success {
		peer.Log.Errorf("receive failed piece: %#v %#v", piece, piece.PieceInfo)
		s.callback.PieceFail(ctx, peer, piece)
		return
	}

	// Handle unknow piece
	peer.Log.Warnf("receive unknow piece: %#v", piece)
	return
}

func (s *service) HandlePeer(ctx context.Context, peer *resource.Peer, req *rpcscheduler.PeerResult) {
	if !req.Success {
		if peer.Task.BackToSourcePeers.Contains(peer) {
			s.callback.TaskFail(ctx, peer.Task)
		}
		s.callback.PeerFail(ctx, peer)
		return
	}

	if peer.Task.BackToSourcePeers.Contains(peer) {
		s.callback.TaskSuccess(ctx, peer.Task, req)
	}
	s.callback.PeerSuccess(ctx, peer)
}

func (s *service) HandlePeerLeave(ctx context.Context, peer *resource.Peer) {
	s.callback.PeerLeave(ctx, peer)
}
