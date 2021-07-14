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
	host2 "d7y.io/dragonfly/v2/scheduler/types/host"
	peer2 "d7y.io/dragonfly/v2/scheduler/types/peer"
	task2 "d7y.io/dragonfly/v2/scheduler/types/task"
	"github.com/panjf2000/ants/v2"
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

	pool *ants.Pool
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
			return nil, err
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
		return nil, err
	}

	pool, err := ants.NewPool(cfg.WorkerNum)
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
		pool:        pool,
		config:      cfg,
	}, nil
}

func (s *SchedulerService) GenerateTaskID(url string, filter string, meta *base.UrlMeta, bizID string, peerID string) (taskID string) {
	if s.config.ABTest {
		return idgen.TwinsTaskID(url, filter, meta, bizID, peerID)
	}
	return idgen.TaskID(url, filter, meta, bizID)
}

func (s *SchedulerService) ScheduleParent(peer *peer2.PeerNode) (parent *peer2.PeerNode, err error) {
	parent, _ = s.scheduler.ScheduleParent(peer, 1)
	return
}

func (s *SchedulerService) GetPeerTask(peerTaskID string) (peerTask *peer2.PeerNode, ok bool) {
	return s.peerManager.Get(peerTaskID)
}

func (s *SchedulerService) RegisterPeerTask(req *schedulerRPC.PeerTaskRequest, task *task2.Task) (*peer2.PeerNode, error) {
	// get or create host
	reqPeerHost := req.PeerHost
	var (
		peerNode *peer2.PeerNode
		ok       bool
		host     *host2.NodeHost
	)

	if host, ok = s.hostManager.Get(reqPeerHost.Uuid); !ok {
		host = &host2.NodeHost{
			UUID:            reqPeerHost.Uuid,
			IP:              reqPeerHost.Ip,
			HostName:        reqPeerHost.HostName,
			RPCPort:         reqPeerHost.RpcPort,
			DownloadPort:    reqPeerHost.DownPort,
			HostType:        host2.PeerNodeHost,
			SecurityDomain:  reqPeerHost.SecurityDomain,
			Location:        reqPeerHost.Location,
			IDC:             reqPeerHost.Idc,
			NetTopology:     reqPeerHost.NetTopology,
			TotalUploadLoad: host2.ClientHostLoad,
		}
		s.hostManager.Add(host)
	}

	// get or creat PeerTask
	if peerNode, ok = s.peerManager.Get(req.PeerId); !ok {
		peerNode = &peer2.PeerNode{
			PeerID:         req.PeerId,
			Task:           task,
			Host:           host,
			FinishedNum:    0,
			LastAccessTime: time.Now(),
			Parent:         nil,
			Children:       nil,
			Status:         peer2.PeerStatusWaiting,
			CostHistory:    nil,
		}
		s.peerManager.Add(peerNode)
	}
	return peerNode, nil
}

func (s *SchedulerService) GetOrCreateTask(ctx context.Context, task *task2.Task) (*task2.Task, error) {
	synclock.Lock(task.TaskID, true)
	task, ok := s.taskManager.GetOrAdd(task)
	task.LastAccessTime = time.Now()
	if ok {
		if task.LastTriggerTime.Add(s.config.AccessWindow).After(time.Now()) || task2.IsHealthTask(task) {
			synclock.UnLock(task.TaskID, true)
			return task, nil
		}
	}
	synclock.UnLock(task.TaskID, true)
	var (
		once        sync.Once
		cdnPeerNode *peer2.PeerNode
		cdnHost     *host2.NodeHost
	)
	synclock.Lock(task.TaskID, false)
	defer synclock.Lock(task.TaskID, false)
	if err := s.cdnManager.StartSeedTask(ctx, task, func(ps *cdnsystem.PieceSeed, err error) {
		once.Do(func() {
			cdnHost, _ = s.hostManager.Get(ps.HostUuid)
			cdnPeerNode = &peer2.PeerNode{
				PeerID:         ps.PeerId,
				Task:           task,
				Host:           cdnHost,
				LastAccessTime: time.Now(),
				Status:         peer2.PeerStatusRunning,
			}
			s.peerManager.Add(cdnPeerNode)
		})
		if err != nil {
			cdnPeerNode.SetStatus(peer2.PeerStatusBadNode)
			return
		}
		cdnPeerNode.FinishedNum++
		cdnPeerNode.LastAccessTime = time.Now()
		if ps.Done {
			cdnPeerNode.Status = peer2.PeerStatusSuccess
			if task.ContentLength <= task2.TinyFileSize {
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
	return s.pool.Submit(s.worker.NewHandleReportPieceResultTask(pieceResult))
}

func (s *SchedulerService) HandlePeerResult(peerResult *schedulerRPC.PeerResult) error {
	return s.pool.Submit(s.worker.NewHandleReportPeerResultTask(peerResult))
}

func (s *SchedulerService) HandleLeaveTask(target *schedulerRPC.PeerTarget) error {
	return s.pool.Submit(s.worker.NewHandleLeaveTask(target))
}
