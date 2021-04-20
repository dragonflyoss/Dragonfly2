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
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/mgr"
	"d7y.io/dragonfly/v2/scheduler/scheduler"
)

type SchedulerService struct {
	cdnMgr      *mgr.CDNManager
	taskMgr     *mgr.TaskManager
	hostMgr     *mgr.HostManager
	peerTaskMgr *mgr.PeerTaskManager
	scheduler   *scheduler.Scheduler
}

func NewSchedulerService(cfg *config.Config) *SchedulerService {
	s := &SchedulerService{
		cdnMgr:      mgr.GetCDNManager(),
		taskMgr:     mgr.GetTaskManager(),
		hostMgr:     mgr.GetHostManager(),
		peerTaskMgr: mgr.GetPeerTaskManager(),
		scheduler:   scheduler.CreateScheduler(),
	}
	s.cdnMgr.InitCDNClient()

	return s
}

func (s *SchedulerService) GetScheduler() *scheduler.Scheduler {
	return s.scheduler
}
