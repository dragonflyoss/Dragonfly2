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
	"time"

	"d7y.io/dragonfly/v2/internal/idgen"
	"d7y.io/dragonfly/v2/internal/rpc/base"
	"d7y.io/dragonfly/v2/internal/rpc/scheduler"
	"d7y.io/dragonfly/v2/pkg/synclock"
	"d7y.io/dragonfly/v2/scheduler/config"
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

	worker *Worker
	config config.SchedulerConfig
	ABTest bool
}

func NewSchedulerService(cfg *config.SchedulerConfig, dynConfig config.DynconfigInterface) (*SchedulerService, error) {
	schedulerConfig, err := dynConfig.Get()
	if err != nil {
		return nil, err
	}
	cdnManager := source.NewManager()
	if cfg.EnableCDN {
		cdnManager, err := d7y.NewManager(schedulerConfig.CdnHosts)
		if err != nil {
			return nil, err
		}
		dynConfig.Register(cdnManager)
	}
	hostManager := host.NewManager()
	taskManager := task.NewManager()
	peerManager := peer.NewManager()
	worker, err := newWorker(cfg, newScheduler(cfg, taskManager), cdnManager, taskManager, hostManager, peerManager)
	if err != nil {
		return nil, err
	}
	return &SchedulerService{
		cdnManager:  cdnManager,
		taskManager: taskManager,
		hostManager: hostManager,
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

func (s *SchedulerService) GetTask(taskID string) (*types.Task, bool) {
	return s.taskManager.Get(taskID)
}

func (s *SchedulerService) ScheduleParent(task *types.PeerNode) (primary *types.PeerNode,
	secondary []*types.PeerNode, err error) {
	return s.worker.ScheduleParent(task)
}

func (s *SchedulerService) GetPeerTask(peerTaskID string) (peerTask *types.PeerNode, ok bool) {
	return s.peerManager.Get(peerTaskID)
}

func (s *SchedulerService) GetPeerChan(peerID string) chan *scheduler.PeerPacket {
	return nil
}

func (s *SchedulerService) RegisterPeerTask(req *scheduler.PeerTaskRequest, task *types.Task) (*types.PeerNode, error) {
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

func (s *SchedulerService) GetOrCreateTask(ctx context.Context, task *types.Task) error {
	synclock.Lock(task.TaskID, true)
	defer synclock.UnLock(task.TaskID, true)
	existTask, ok := s.taskManager.Get(task.TaskID)
	if ok {
		if existTask.LastTriggerTime.Add(2 * time.Minute).After(time.Now()) {
			return existTask
		}
	}
	err := s.cdnManager.SeedTask(ctx, task)
	if err != nil {
		// todo 针对cdn的错误进行处理
		return err
	}

	s.peerManager.Add(&types.PeerNode{
		PeerID:         "",
		Task:           task,
		Host:           nil,
		FinishedNum:    0,
		StartTime:      time.Time{},
		LastAccessTime: time.Time{},
		Parent:         nil,
		Children:       nil,
		Success:        false,
		Status:         0,
		CostHistory:    nil,
	})
	// todo register cdn peer task
	task.Status = types.TaskStatusRunning
	s.taskManager.Add(task)

	s.peerManager.Add(peerNode)
	return nil
}

func (s *SchedulerService) HandlePieceResult(pieceResult *scheduler.PieceResult) {
	s.worker.Submit(NewReportPieceResultTask(s.worker, pieceResult))
}

func (s *SchedulerService) HandlePeerResult(result *scheduler.PeerResult) error {
	return s.worker.Submit(NewReportPeerResultTask(s.worker, result))
}

func (s *SchedulerService) HandleLeaveTask(target *scheduler.PeerTarget) error {
	s.worker.Submit(NewLeaveTask(s.worker, target))
}

func (s *SchedulerService) monitor() {

}

func (s *SchedulerService) AddPieceStream(peerID string, packetChan chan *scheduler.PeerPacket) {

}
