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
	"errors"

	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/manager"
	"d7y.io/dragonfly/v2/scheduler/scheduler"
	"d7y.io/dragonfly/v2/scheduler/types"
)

type SchedulerService struct {
	CDNManager  *manager.CDNManager
	TaskManager *manager.TaskManager
	HostManager *manager.HostManager
	Scheduler   *scheduler.Scheduler
	config      config.SchedulerConfig
	ABTest      bool
}

func NewSchedulerService(cfg *config.Config) *SchedulerService {
	mgr := manager.New(cfg)
	return &SchedulerService{
		CDNManager:  mgr.CDNManager,
		TaskManager: mgr.TaskManager,
		HostManager: mgr.HostManager,
		Scheduler:   scheduler.New(),
		ABTest:      cfg.Scheduler.ABTest,
	}
}

func (s *SchedulerService) GenerateTaskId(url string, filter string, meta *base.UrlMeta, bizId string, peerId string) (taskId string) {
	if s.ABTest {
		return idgen.GenerateTwinsTaskId(url, filter, meta, bizId, peerId)
	}
	return idgen.GenerateTaskId(url, filter, meta, bizId)
}

func (s *SchedulerService) GetTask(taskId string) (task *types.Task, err error) {
	task, _ = s.TaskManager.Get(taskId)
	if task == nil {
		err = errors.New("peer task not exited: " + taskId)
	}
	return
}

func (s *SchedulerService) AddTask(task *types.Task) (ret *types.Task, err error) {
	ret, added := s.TaskManager.Add(task)
	if added {
		err = s.CDNManager.TriggerTask(ret, s.TaskManager.PeerTask.CDNCallback)
	}
	s.TaskManager.PeerTask.AddTask(ret)
	return
}

func (s *SchedulerService) ScheduleParent(task *types.PeerTask) (primary *types.PeerTask,
	secondary []*types.PeerTask, err error) {
	return s.Scheduler.ScheduleParent(task)
}

func (s *SchedulerService) ScheduleChildren(task *types.PeerTask) (children []*types.PeerTask, err error) {
	return s.Scheduler.ScheduleChildren(task)
}

func (s *SchedulerService) GetPeerTask(peerTaskId string) (peerTask *types.PeerTask, err error) {
	peerTask, _ = s.TaskManager.PeerTask.Get(peerTaskId)
	if peerTask == nil {
		err = errors.New("peer task do not exist: " + peerTaskId)
	}
	return
}

func (s *SchedulerService) AddPeerTask(pid string, task *types.Task, host *types.Host) (ret *types.PeerTask, err error) {
	ret = s.TaskManager.PeerTask.Add(pid, task, host)
	host.AddPeerTask(ret)
	return
}

func (s *SchedulerService) DeletePeerTask(peerTaskId string) (err error) {
	peerTask, err := s.GetPeerTask(peerTaskId)
	if err != nil {
		return
	}
	// delete from manager
	s.TaskManager.PeerTask.Delete(peerTaskId)
	// delete from host
	peerTask.Host.DeletePeerTask(peerTaskId)
	// delete from piece lazy
	peerTask.SetDown()
	return
}

func (s *SchedulerService) GetHost(hostId string) (host *types.Host, err error) {
	host, _ = s.HostManager.Get(hostId)
	if host == nil {
		err = errors.New("host not exited: " + hostId)
	}
	return
}

func (s *SchedulerService) AddHost(host *types.Host) (ret *types.Host, err error) {
	ret = s.HostManager.Add(host)
	return
}
