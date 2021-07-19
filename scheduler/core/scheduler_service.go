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

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
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
	"github.com/panjf2000/ants/v2"
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

	jobFactory *JobFactory
	pool       *ants.Pool
	//pool      *Worker
	config    *config.SchedulerConfig
	scheduler scheduler.Scheduler
	wg        sync.WaitGroup
}

func NewSchedulerService(cfg *config.SchedulerConfig, dynConfig config.DynconfigInterface) (*SchedulerService, error) {
	schedulerConfig, err := dynConfig.Get()
	if err != nil {
		return nil, err
	}
	hostManager := host.NewManager()
	peerManager := peer.NewManager(cfg.GC, hostManager)
	cdnManager := source.NewManager()
	if cfg.EnableCDN {
		cdnManager, err = d7y.NewManager(schedulerConfig.Cdns, peerManager, hostManager)
		if err != nil {
			return nil, errors.Wrap(err, "new cdn manager")
		}
		dynConfig.Register(cdnManager)
		hostManager.OnNotify(schedulerConfig)
		dynConfig.Register(hostManager)
	}
	taskManager := task.NewManager()
	scheduler, err := scheduler.Get(cfg.Scheduler).Build(cfg, &scheduler.BuildOptions{
		PeerManager: peerManager,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "build scheduler %v", cfg.Scheduler)
	}
	jf, err := newJobFactory(scheduler, cdnManager, taskManager, hostManager, peerManager)
	if err != nil {
		return nil, errors.Wrap(err, "new mission factory")
	}
	if cfg.OpenMonitor {
		NewMonitor(peerManager)
	}
	return &SchedulerService{
		cdnManager:  cdnManager,
		taskManager: taskManager,
		hostManager: hostManager,
		scheduler:   scheduler,
		jobFactory:  jf,
		config:      cfg,
	}, nil
}

func (s *SchedulerService) GenerateTaskID(url string, filter string, meta *base.UrlMeta, bizID string, peerID string) (taskID string) {
	if s.config.ABTest {
		return idgen.TwinsTaskID(url, filter, meta, bizID, peerID)
	}
	return idgen.TaskID(url, filter, meta, bizID)
}

func (s *SchedulerService) ScheduleParent(peer *types.Peer) (parent *types.Peer, err error) {
	parent, _ = s.scheduler.ScheduleParent(peer)
	return
}

func (s *SchedulerService) GetPeerTask(peerTaskID string) (peerTask *types.Peer, ok bool) {
	return s.peerManager.Get(peerTaskID)
}

func (s *SchedulerService) RegisterPeerTask(req *schedulerRPC.PeerTaskRequest, task *types.Task) (*types.Peer, error) {
	// get or create host
	reqPeerHost := req.PeerHost
	var (
		peer *types.Peer
		ok   bool
		host *types.PeerHost
	)

	if host, ok = s.hostManager.Get(reqPeerHost.Uuid); !ok {
		host = &types.PeerHost{
			UUID:            reqPeerHost.Uuid,
			IP:              reqPeerHost.Ip,
			HostName:        reqPeerHost.HostName,
			RPCPort:         reqPeerHost.RpcPort,
			DownloadPort:    reqPeerHost.DownPort,
			CDN:             false,
			SecurityDomain:  reqPeerHost.SecurityDomain,
			Location:        reqPeerHost.Location,
			IDC:             reqPeerHost.Idc,
			NetTopology:     reqPeerHost.NetTopology,
			TotalUploadLoad: s.config.ClientLoad,
		}
		s.hostManager.Add(host)
	}

	// get or creat PeerTask
	if peer, ok = s.peerManager.Get(req.PeerId); !ok {
		peer = &types.Peer{
			PeerID: req.PeerId,
			Task:   task,
			Host:   host,
		}
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
	defer synclock.Lock(task.TaskID, false)
	if !task.IsHealth() {
		task.SetStatus(types.TaskStatusRunning)
	}
	go func() {
		if err := s.cdnManager.StartSeedTask(ctx, task, false); err != nil {
			logger.Errorf("failed to seed task: %v", err)
			if err = s.pool.Submit(s.jobFactory.NewHandleFailSeedTaskJob(task)); err != nil {
				logger.Errorf("failed to submit handle fail seed task job: %v", err)
			}
		}
	}()
	return task, nil
}

func (s *SchedulerService) HandlePieceResult(peer *types.Peer, pieceResult *schedulerRPC.PieceResult) error {
	job := s.jobFactory.NewHandleReportPieceResultJob(peer, pieceResult)
	if job != nil {
		return s.pool.Submit(job)
	}
	return nil
}

func (s *SchedulerService) HandlePeerResult(peer *types.Peer, peerResult *schedulerRPC.PeerResult) error {
	job := s.jobFactory.NewHandleReportPeerResultJob(peer, peerResult)
	if job != nil {
		return s.pool.Submit(job)
	}
	return nil
}

func (s *SchedulerService) HandleLeaveTask(peer *types.Peer) error {
	job := s.jobFactory.NewHandleLeaveJob(peer)
	if job != nil {
		return s.pool.Submit(job)
	}
	return nil
}
