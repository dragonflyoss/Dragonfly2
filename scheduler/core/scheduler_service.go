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

package core

import (
	"context"
	"sync"
	"time"

	"d7y.io/dragonfly/v2/internal/dfcodes"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/base/common"
	schedulerRPC "d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/pkg/synclock"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/core/scheduler"
	"d7y.io/dragonfly/v2/scheduler/daemon"
	"d7y.io/dragonfly/v2/scheduler/daemon/cdn/d7y"
	"d7y.io/dragonfly/v2/scheduler/daemon/cdn/source"
	"d7y.io/dragonfly/v2/scheduler/daemon/host"
	"d7y.io/dragonfly/v2/scheduler/daemon/peer"
	"d7y.io/dragonfly/v2/scheduler/daemon/task"
	"d7y.io/dragonfly/v2/scheduler/types"
	"github.com/pkg/errors"
)

type SchedulerService struct {
	// cdn mgr
	cdnManager daemon.CDNMgr
	// task mgr
	taskManager daemon.TaskMgr
	// host mgr
	hostManager daemon.HostMgr
	// Peer mgr
	peerManager daemon.PeerMgr

	sched    scheduler.Scheduler
	worker   worker
	config   *config.SchedulerConfig
	wg       sync.WaitGroup
	monitor  *monitor
	stopOnce sync.Once
}

func NewSchedulerService(cfg *config.SchedulerConfig, dynConfig config.DynconfigInterface) (*SchedulerService, error) {
	dynConfigData, err := dynConfig.Get()
	if err != nil {
		return nil, err
	}
	hostManager := host.NewManager()
	peerManager := peer.NewManager(cfg.GC, hostManager)
	cdnManager := source.NewManager()
	if !cfg.DisableCDN {
		cdnManager, err = d7y.NewManager(dynConfigData.CDNs, peerManager, hostManager)
		if err != nil {
			return nil, errors.Wrap(err, "new cdn manager")
		}
		dynConfig.Register(cdnManager)
		hostManager.OnNotify(dynConfigData)
		dynConfig.Register(hostManager)
	}
	taskManager := task.NewManager(cfg.GC)
	sched, err := scheduler.Get(cfg.Scheduler).Build(cfg, &scheduler.BuildOptions{
		PeerManager: peerManager,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "build scheduler %v", cfg.Scheduler)
	}

	work := newEventLoopGroup(cfg.WorkerNum)
	var downloadMonitor *monitor
	if cfg.OpenMonitor {
		downloadMonitor = newMonitor(peerManager)
	}
	return &SchedulerService{
		cdnManager:  cdnManager,
		taskManager: taskManager,
		hostManager: hostManager,
		peerManager: peerManager,
		worker:      work,
		monitor:     downloadMonitor,
		sched:       sched,
		config:      cfg,
	}, nil
}

func (s *SchedulerService) Serve() {
	go s.worker.start(newState(s.sched, s.peerManager, s.cdnManager))
	if s.monitor != nil {
		go s.monitor.start()
	}
	logger.Debugf("start scheduler service successfully")
}

func (s *SchedulerService) Stop() {
	s.stopOnce.Do(func() {
		if s.worker != nil {
			s.worker.stop()
		}
		if s.monitor != nil {
			s.monitor.stop()
		}
	})
}

func (s *SchedulerService) GenerateTaskID(url string, filter string, meta *base.UrlMeta, bizID string, peerID string) (taskID string) {
	if s.config.ABTest {
		return idgen.TwinsTaskID(url, filter, meta, bizID, peerID)
	}
	return idgen.TaskID(url, filter, meta, bizID)
}

func (s *SchedulerService) ScheduleParent(peer *types.Peer) (parent *types.Peer, err error) {
	parent, _, hasParent := s.sched.ScheduleParent(peer)
	//logger.Debugf("schedule parent result: parent %v, candidates:%v", parent, candidates)
	if !hasParent || parent == nil {
		return nil, errors.Errorf("no parent peer available for peer %v", peer.PeerID)
	}
	return parent, nil
}

func (s *SchedulerService) GetPeerTask(peerTaskID string) (peerTask *types.Peer, ok bool) {
	return s.peerManager.Get(peerTaskID)
}

func (s *SchedulerService) RegisterPeerTask(req *schedulerRPC.PeerTaskRequest, task *types.Task) (*types.Peer, error) {
	// get or create host
	reqPeerHost := req.PeerHost
	var (
		peer     *types.Peer
		ok       bool
		peerHost *types.PeerHost
	)

	if peerHost, ok = s.hostManager.Get(reqPeerHost.Uuid); !ok {
		peerHost = types.NewClientPeerHost(reqPeerHost.Uuid, reqPeerHost.Ip, reqPeerHost.HostName, reqPeerHost.RpcPort, reqPeerHost.DownPort,
			reqPeerHost.SecurityDomain, reqPeerHost.Location, reqPeerHost.Idc, reqPeerHost.NetTopology, s.config.ClientLoad)
		s.hostManager.Add(peerHost)
	}

	// get or creat PeerTask
	if peer, ok = s.peerManager.Get(req.PeerId); !ok {
		peer = types.NewPeer(req.PeerId, task, peerHost)
		s.peerManager.Add(peer)
	}
	return peer, nil
}

func (s *SchedulerService) GetOrCreateTask(ctx context.Context, task *types.Task) (*types.Task, error) {
	synclock.Lock(task.TaskID, true)
	task, ok := s.taskManager.GetOrAdd(task)
	if ok {
		if task.GetLastTriggerTime().Add(s.config.AccessWindow).After(time.Now()) || task.IsHealth() {
			synclock.UnLock(task.TaskID, true)
			return task, nil
		}
	}
	synclock.UnLock(task.TaskID, true)
	// do trigger
	task.SetLastTriggerTime(time.Now())
	// register cdn peer task
	// notify peer tasks
	synclock.Lock(task.TaskID, false)
	defer synclock.UnLock(task.TaskID, false)
	if task.IsHealth() && task.GetLastTriggerTime().Add(s.config.AccessWindow).After(time.Now()) {
		return task, nil
	}
	if task.IsFrozen() {
		task.SetStatus(types.TaskStatusRunning)
	}
	go func() {
		if err := s.cdnManager.StartSeedTask(ctx, task); err != nil {
			if !task.IsSuccess() {
				task.SetStatus(types.TaskStatusFailed)
			}
			logger.Errorf("failed to seed task: %v", err)
			if ok = s.worker.send(taskSeedFailEvent{task}); !ok {
				logger.Error("failed to send taskSeed fail event, eventLoop is shutdown")
			}
		} else {
			logger.Debugf("===== successfully obtain seeds from cdn, task: %+v ====", task)
		}
	}()
	return task, nil
}

func (s *SchedulerService) HandlePieceResult(peer *types.Peer, pieceResult *schedulerRPC.PieceResult) error {
	peer.Touch()
	if pieceResult.PieceNum == common.ZeroOfPiece {
		s.worker.send(startReportPieceResultEvent{peer})
		return nil
	} else if pieceResult.Success {
		s.worker.send(peerDownloadPieceSuccessEvent{
			peer: peer,
			pr:   pieceResult,
		})
		return nil
	} else if pieceResult.Code != dfcodes.Success {
		s.worker.send(peerDownloadPieceFailEvent{
			peer: peer,
			pr:   pieceResult,
		})
		return nil
	}
	return nil
}

func (s *SchedulerService) HandlePeerResult(peer *types.Peer, peerResult *schedulerRPC.PeerResult) error {
	peer.Touch()
	if peerResult.Success {
		if !s.worker.send(peerDownloadSuccessEvent{peer: peer, peerResult: peerResult}) {
			logger.Errorf("send peer download success event failed")
		}
	} else if !s.worker.send(peerDownloadFailEvent{peer: peer, peerResult: peerResult}) {
		logger.Errorf("send peer download fail event failed")
	}
	return nil
}

func (s *SchedulerService) HandleLeaveTask(peer *types.Peer) error {
	peer.Touch()
	if !s.worker.send(peerLeaveEvent{peer: peer}) {
		logger.Errorf("send peer leave event failed")
	}
	return nil
}
