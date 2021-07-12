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

	worker    *Worker
	config    config.SchedulerConfig
	scheduler scheduler.Scheduler
	ABTest    bool
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
			return nil, err
		}
		dynConfig.Register(cdnManager)
		dynConfig.Register(hostManager)
	}
	taskManager := task.NewManager()
	peerManager := peer.NewManager()
	scheduler, err := scheduler.Get("basic").Build(&scheduler.BuildOptions{
		PeerManager: peerManager,
	})
	if err != nil {
		return nil, err
	}
	worker, err := newWorker(cfg, scheduler, cdnManager, taskManager, hostManager, peerManager)
	if err != nil {
		return nil, err
	}
	return &SchedulerService{
		cdnManager:  cdnManager,
		taskManager: taskManager,
		hostManager: hostManager,
		scheduler:   scheduler,
		worker:      worker,
		ABTest:      cfg.ABTest,
	}, nil
}

func (s *SchedulerService) GenerateTaskID(url string, filter string, meta *base.UrlMeta, bizID string, peerID string) (taskID string) {
	if s.ABTest {
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
			TotalUploadLoad: 0,
		}
		s.hostManager.Add(host)
	}

	// get or creat PeerTask
	if peerNode, ok = s.peerManager.Get(req.PeerId); !ok {
		peerNode = &types.PeerNode{
			PeerID:         req.PeerId,
			Task:           task,
			Host:           host,
			FinishedNum:    0,
			StartTime:      time.Now(),
			LastAccessTime: time.Now(),
			Parent:         nil,
			Children:       nil,
			Success:        false,
			Status:         0,
			CostHistory:    nil,
		}
		s.peerManager.Add(peerNode)
	}
	return peerNode, nil
}

func (s *SchedulerService) GetOrCreateTask(ctx context.Context, task *types.Task) (*types.Task, error) {
	synclock.Lock(task.TaskID, true)
	defer synclock.UnLock(task.TaskID, true)
	existTask, ok := s.taskManager.Get(task.TaskID)
	if ok {
		if existTask.LastTriggerTime.Add(s.config.AccessWindow).After(time.Now()) && !types.IsFrozenTask(task) {
			return existTask, nil
		}
	}
	var (
		once        sync.Once
		cdnPeerNode *types.PeerNode
		cdnHost     *types.NodeHost
	)
	s.taskManager.Add(task)
	err := s.cdnManager.StartSeedTask(ctx, task, func(ps *cdnsystem.PieceSeed) {
		once.Do(func() {
			cdnHost, _ = s.hostManager.Get(ps.HostUuid)
			cdnPeerNode = &types.PeerNode{
				PeerID:         ps.PeerId,
				Task:           task,
				Host:           cdnHost,
				StartTime:      time.Now(),
				LastAccessTime: time.Now(),
				Success:        false,
				Status:         types.PeerStatusWaiting,
			}
			s.peerManager.Add(cdnPeerNode)
		})
		if types.IsFrozenTask(task) {
			cdnPeerNode.FinishedNum++
			cdnPeerNode.LastAccessTime = time.Now()
		}
		if ps.Done {
			cdnPeerNode.Status = types.PeerStatusSuccess
		}
		if task.PieceTotal == 1 {
			if task.ContentLength <= types.TinyFileSize {
				content, er := s.cdnManager.DownloadTinyFileContent(task, cdnHost)
				if er == nil && len(content) == int(task.ContentLength) {
					task.DirectPiece = content
				}
			}
		}
	})
	if err != nil {
		return nil, err
	}
	return task, nil
}

func (s *SchedulerService) HandlePieceResult(pieceResult *schedulerRPC.PieceResult) {
	s.worker.Submit(NewReportPieceResultTask(s.worker, pieceResult))
}

func (s *SchedulerService) HandlePeerResult(result *schedulerRPC.PeerResult) error {
	return s.worker.Submit(NewReportPeerResultTask(s.worker, result))
}

func (s *SchedulerService) HandleLeaveTask(target *schedulerRPC.PeerTarget) error {
	return s.worker.Submit(NewLeaveTask(s.worker, target))
}
