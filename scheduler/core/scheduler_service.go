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

	"d7y.io/dragonfly/v2/internal/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem"
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

	missionFactory *missionFactory
	worker         *worker
	pool           *ants.Pool
	//pool      *Worker
	config    *config.SchedulerConfig
	scheduler scheduler.Scheduler
}

func NewSchedulerService(cfg *config.SchedulerConfig, dynConfig config.DynconfigInterface) (*SchedulerService, error) {
	schedulerConfig, err := dynConfig.Get()
	if err != nil {
		return nil, err
	}
	cdnManager := source.NewManager()
	hostManager := host.NewManager()
	if cfg.EnableCDN {
		cdnManager, err = d7y.NewManager(schedulerConfig.Cdns)
		if err != nil {
			return nil, errors.Wrap(err, "new cdn manager")
		}
		dynConfig.Register(cdnManager)
		hostManager.OnNotify(schedulerConfig)
		dynConfig.Register(hostManager)
	}
	taskManager := task.NewManager()
	peerManager := peer.NewManager()
	scheduler, err := scheduler.Get(cfg.Scheduler).Build(cfg, &scheduler.BuildOptions{
		PeerManager: peerManager,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "build scheduler %s", cfg.Scheduler)
	}
	mf, err := newMissionFactory(scheduler, cdnManager, taskManager, hostManager, peerManager)
	if err != nil {
		return nil, errors.Wrap(err, "new mission factory")
	}
	worker, err := newWorker(cfg.WorkerNum)
	if err != nil {
		return nil, errors.Wrap(err, "new worker")
	}
	return &SchedulerService{
		cdnManager:     cdnManager,
		taskManager:    taskManager,
		hostManager:    hostManager,
		scheduler:      scheduler,
		missionFactory: mf,
		worker:         worker,
		config:         cfg,
	}, nil
}

func (s *SchedulerService) GenerateTaskID(url string, filter string, meta *base.UrlMeta, bizID string, peerID string) (taskID string) {
	if s.config.ABTest {
		return idgen.TwinsTaskID(url, filter, meta, bizID, peerID)
	}
	return idgen.TaskID(url, filter, meta, bizID)
}

func (s *SchedulerService) ScheduleParent(peer *types.PeerNode) (parent *types.PeerNode, err error) {
	parent, _ = s.scheduler.ScheduleParent(peer, 1)
	return
}

func (s *SchedulerService) GetPeerTask(peerTaskID string) (peerTask *types.PeerNode, ok bool) {
	return s.peerManager.Get(peerTaskID)
}

func (s *SchedulerService) RegisterPeerTask(req *schedulerRPC.PeerTaskRequest, task *types.Task) (*types.PeerNode, error) {
	// get or create host
	reqPeerHost := req.PeerHost
	var (
		peerNode *types.PeerNode
		ok       bool
		host     *types.NodeHost
	)

	if host, ok = s.hostManager.Get(reqPeerHost.Uuid); !ok {
		host = &types.NodeHost{
			UUID:            reqPeerHost.Uuid,
			IP:              reqPeerHost.Ip,
			HostName:        reqPeerHost.HostName,
			RPCPort:         reqPeerHost.RpcPort,
			DownloadPort:    reqPeerHost.DownPort,
			HostType:        types.PeerNodeHost,
			SecurityDomain:  reqPeerHost.SecurityDomain,
			Location:        reqPeerHost.Location,
			IDC:             reqPeerHost.Idc,
			NetTopology:     reqPeerHost.NetTopology,
			TotalUploadLoad: types.ClientHostLoad,
		}
		s.hostManager.Add(host)
	}

	// get or creat PeerTask
	if peerNode, ok = s.peerManager.Get(req.PeerId); !ok {
		peerNode = &types.PeerNode{
			PeerID: req.PeerId,
			Task:   task,
			Host:   host,
		}
		s.peerManager.Add(peerNode)
	}
	return peerNode, nil
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
	var (
		once        sync.Once
		cdnPeerNode *types.PeerNode
		cdnHost     *types.NodeHost
	)
	synclock.Lock(task.TaskID, false)
	defer synclock.Lock(task.TaskID, false)
	if err := s.cdnManager.StartSeedTask(ctx, task, func(ps *cdnsystem.PieceSeed, err error) {
		once.Do(func() {
			cdnHost, _ = s.hostManager.Get(ps.HostUuid)
			s.peerManager.Add(types.NewPeerNode(ps.PeerId, task, cdnHost))
		})
		if err != nil {
			cdnPeerNode.SetStatus(types.PeerStatusBadNode)
			return
		}
		cdnPeerNode.IncFinishNum()
		cdnPeerNode.Touch()
		if ps.Done {
			cdnPeerNode.SetStatus(types.PeerStatusSuccess)
			if task.ContentLength <= types.TinyFileSize {
				content, er := s.cdnManager.DownloadTinyFileContent(task, cdnHost)
				if er == nil && len(content) == int(task.ContentLength) {
					task.DirectPiece = content
				}
			}
		}
	}); err != nil {
		return nil, err
	}
	return task, nil
}

func (s *SchedulerService) HandlePieceResult(pieceResult *schedulerRPC.PieceResult) error {
	return s.pool.Submit(s.missionFactory.NewHandleReportPieceResultMission(pieceResult))
}

func (s *SchedulerService) HandlePeerResult(peerResult *schedulerRPC.PeerResult) error {
	return s.pool.Submit(s.missionFactory.NewHandleReportPeerResultMission(peerResult))
}

func (s *SchedulerService) HandleLeaveTask(target *schedulerRPC.PeerTarget) error {
	return s.pool.Submit(s.missionFactory.NewHandleLeaveMission(target))
}
