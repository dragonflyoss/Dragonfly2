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
	"d7y.io/dragonfly/v2/scheduler/types"
)

type SchedulerService struct {
	// cdn mgr
	CDNManager daemon.CDNMgr
	// task mgr
	TaskManager daemon.TaskMgr
	// host mgr
	HostManager daemon.HostMgr
	// Peer mgr
	PeerManager daemon.PeerMgr

	Scheduler *Scheduler
	config    config.SchedulerConfig
	ABTest    bool
}

func NewSchedulerService(cfg *config.Config, dynconfig config.DynconfigInterface) (*SchedulerService, error) {
	mgr, err := manager.New(cfg, dynconfig)
	if err != nil {
		return nil, err
	}

	return &SchedulerService{
		CDNManager:  daemon.CDNMgr(),
		TaskManager: mgr.TaskManager,
		HostManager: mgr.HostManager,
		Scheduler:   scheduler.New(cfg.Scheduler, mgr.TaskManager),
		ABTest:      cfg.Scheduler.ABTest,
	}, nil
}

func (s *SchedulerService) GenerateTaskID(url string, filter string, meta *base.UrlMeta, bizID string, peerID string) (taskID string) {
	if s.ABTest {
		return idgen.TwinsTaskID(url, filter, meta, bizID, peerID)
	}
	return idgen.TaskID(url, filter, meta, bizID)
}

func (s *SchedulerService) GetTask(taskID string) (*types.Task, bool) {
	return s.TaskManager.Get(taskID)
}

func (s *SchedulerService) ScheduleParent(task *types.PeerNode) (primary *types.PeerNode,
	secondary []*types.PeerNode, err error) {
	return s.Scheduler.ScheduleParent(task)
}

func (s *SchedulerService) ScheduleChildren(task *types.PeerNode) (children []*types.PeerNode, err error) {
	return s.Scheduler.ScheduleChildren(task)
}

func (s *SchedulerService) GetPeerTask(peerTaskID string) (peerTask *types.PeerNode, ok bool) {
	return s.PeerManager.Get(peerTaskID)
}

func (s *SchedulerService) AddPeerTask(pid string, task *types.Task, host *types.NodeHost) (ret *types.PeerNode, err error) {
	ret = s.PeerManager.Add(pid, task, host)
	host.AddPeerTask(ret)
	return
}

func (s *SchedulerService) DeletePeerTask(peerTaskID string) (err error) {
	peerTask, err := s.GetPeerTask(peerTaskID)
	if err != nil {
		return
	}
	// delete from manager
	s.TaskManager.PeerTask.Delete(peerTaskID)
	// delete from host
	peerTask.Host.DeletePeerTask(peerTaskID)
	// delete from piece lazy
	peerTask.SetDown()
	return
}

func (s *SchedulerService) GetHost(hostID string) (host *types.NodeHost, ok bool) {
	return s.HostManager.Get(hostID)
}

func (s *SchedulerService) AddHost(host *types.Host) (ret *types.NodeHost, err error) {
	ret = s.HostManager.Store(host)
	return
}

func (s *SchedulerService) RegisterPeerTask(req *scheduler.PeerTaskRequest, task *types.Task) (*types.PeerNode, error) {
	// get or create host
	reqPeerHost := req.PeerHost
	var (
		peerNode *types.PeerNode
		ok       bool
		host     *types.NodeHost
	)

	if host, ok = s.HostManager.Get(reqPeerHost.Uuid); !ok {
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
		s.HostManager.Add(host)
	}

	// get or creat PeerTask
	if peerNode, ok = s.PeerManager.Get(req.PeerId); !ok {
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
		s.PeerManager.Add(peerNode)
	}
	return peerNode, nil
}

func (s *SchedulerService) GetOrCreateTask(ctx context.Context, task *types.Task) (*types.Task, error) {
	synclock.Lock(task.TaskID, true)
	defer synclock.UnLock(task.TaskID, true)
	existTask, ok := s.TaskManager.Get(task.TaskID)
	if ok {
		if existTask.LastTriggerTime.Add(2 * time.Minute).After(time.Now()) {
			return existTask
		}
	}
	peerNode, err := s.CDNManager.SeedTask(ctx, task)
	if err != nil {
		// todo 针对cdn的错误进行处理
		return err
	}

	task.Status = types.TaskStatusRunning
	s.TaskManager.Add(task)

	s.PeerManager.Add(peerNode)
	return nil
}

func (s *SchedulerService) GCPeerNode(ctx context.Context, peer *types.PeerNode) {
	s.PeerManager.Delete(peer.PeerID)

}
