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

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

	"d7y.io/dragonfly/v2/pkg/container/set"
	"d7y.io/dragonfly/v2/pkg/gc"
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
	ScheduleParent(context.Context, *entity.Peer) (*entity.Peer, []*entity.Peer, bool)
	GetOrAddTask(context.Context, *rpcscheduler.PeerTaskRequest, int32) *entity.Task
	GetOrAddHost(context.Context, *rpcscheduler.PeerTaskRequest) *entity.Host
	GetOrAddPeer(context.Context, *rpcscheduler.PeerTaskRequest, *entity.Task, *entity.Host) *entity.Peer
	GetPeer(string) (*entity.Peer, bool)
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

func New(cfg *config.Config, pluginDir string, gc gc.GC, dynconfig config.DynconfigInterface, openTel bool) (*service, error) {
	// Initialize manager
	var options []grpc.DialOption
	if openTel {
		options = []grpc.DialOption{
			grpc.WithChainUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
			grpc.WithChainStreamInterceptor(otelgrpc.StreamClientInterceptor()),
		}
	}
	manager, err := manager.New(cfg, gc, dynconfig, options)
	if err != nil {
		return nil, err
	}

	// Initialize scheduler
	scheduler := scheduler.New(cfg.Scheduler, pluginDir)

	// Initialize callback
	callback := newCallback(manager, scheduler)

	return &service{
		manager:   manager,
		scheduler: scheduler,
		callback:  callback,
		config:    cfg,
		dynconfig: dynconfig,
		kmu:       pkgsync.NewKrwmutex(),
	}, nil
}

func (s *service) ScheduleParent(ctx context.Context, peer *entity.Peer) (*entity.Peer, []*entity.Peer, bool) {
	return s.scheduler.ScheduleParent(ctx, peer, set.NewSafeSet())
}

func (s *service) GetOrAddTask(ctx context.Context, req *rpcscheduler.PeerTaskRequest, backSourceCount int32) *entity.Task {
	task := entity.NewTask(idgen.TaskID(req.Url, req.UrlMeta), req.Url, backSourceCount, req.UrlMeta)

	s.kmu.RLock(task.ID)
	task, ok := s.manager.Task.GetOrAdd(task)
	if ok && task.IsHealth() {
		// task is healthy and can be reused
		task.LastAccessAt.Store(time.Now())
		task.Log().Infof("reuse task and status is %s", task.GetStatus())
		s.kmu.RUnlock(task.ID)
		return task
	}
	s.kmu.RUnlock(task.ID)

	s.kmu.Lock(task.ID)
	defer s.kmu.Unlock(task.ID)

	// trigger task
	task.SetStatus(entity.TaskStatusRunning)
	task.LastAccessAt.Store(time.Now())

	// start seed cdn task
	go func() {
		task.Log().Info("cdn start seed task")
		peer, result, err := s.manager.CDN.TriggerTask(ctx, task)
		if err != nil {
			// cdn seed task failed
			task.Log().Errorf("cdn seed task failed: %v", err)
			s.callback.TaskFail(ctx, task)
			return
		}

		s.callback.PeerSuccess(ctx, peer, result)
		peer.Log().Infof("successfully obtain seeds from cdn, task: %#v", task)
	}()

	return task
}

func (s *service) GetOrAddHost(ctx context.Context, req *rpcscheduler.PeerTaskRequest) *entity.Host {
	rawHost := req.PeerHost
	host, ok := s.manager.Host.Get(rawHost.Uuid)
	if !ok {
		// Get scheduler cluster client config by manager
		var options []entity.HostOption
		if clientConfig, ok := s.dynconfig.GetSchedulerClusterClientConfig(); ok {
			options = append(options, entity.WithTotalUploadLoad(clientConfig.LoadLimit))
		}

		s.manager.Host.Add(entity.NewHost(rawHost, options...))
		host.Log().Info("create host")
	}

	host.Log().Info("host already exists")
	return host
}

func (s *service) GetOrAddPeer(ctx context.Context, req *rpcscheduler.PeerTaskRequest, task *entity.Task, host *entity.Host) *entity.Peer {
	peer, ok := s.manager.Peer.Get(req.PeerId)
	if !ok {
		peer = entity.NewPeer(req.PeerId, task, host)
		s.manager.Peer.Add(peer)
		peer.Log().Info("create peer")
		return peer
	}

	peer.Log().Info("peer already exists")
	return peer
}

func (s *service) GetPeer(id string) (*entity.Peer, bool) {
	return s.manager.Peer.Get(id)
}

func (s *service) HandlePiece(ctx context.Context, peer *entity.Peer, piece *rpcscheduler.PieceResult) {
	// Handle piece download successfully
	if piece.Success {
		s.callback.PieceSuccess(ctx, peer, piece)

		// Collect peer host traffic metrics
		if s.config.Metrics != nil && s.config.Metrics.EnablePeerHost {
			metrics.PeerHostTraffic.WithLabelValues("download", peer.Host.UUID, peer.Host.IP).Add(float64(piece.PieceInfo.RangeSize))
			if p, ok := s.manager.Peer.Get(piece.DstPid); ok {
				metrics.PeerHostTraffic.WithLabelValues("upload", p.Host.UUID, p.Host.IP).Add(float64(piece.PieceInfo.RangeSize))
			} else {
				peer.Log().Warnf("dst peer %s not found for piece %#v, pieceInfo %#v", piece.DstPid, piece, piece.PieceInfo)
			}
		}
		return
	}

	// Handle end of piece and begin of piece
	if piece.PieceInfo != nil {
		if piece.PieceInfo.PieceNum == common.EndOfPiece {
			peer.Log().Info("receive end of piece")
			return
		}

		if piece.PieceInfo.PieceNum == common.BeginOfPiece {
			s.callback.InitReport(ctx, peer)
			peer.Log().Info("receive begin of piece")
			return
		}
	}

	// Handle piece download failed
	if piece.Code != base.Code_Success {
		s.callback.PieceFail(ctx, peer, piece)
		peer.Log().Infof("receive piece error: %v", piece.Code)
		return
	}
}

func (s *service) HandlePeer(ctx context.Context, peer *entity.Peer, peerResult *rpcscheduler.PeerResult) error {
	peer.LastAccessAt.Store(time.Now())
	if !peerResult.Success {
		s.callback.PeerFail(ctx, peer, peerResult)
	}
	s.callback.PeerSuccess(ctx, peer, peerResult)
	return nil
}

func (s *service) HandlePeerLeave(ctx context.Context, peer *entity.Peer) error {
	peer.LastAccessAt.Store(time.Now())
	return nil
}
