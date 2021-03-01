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
	"d7y.io/dragonfly/v2/scheduler/types"
	"errors"
)

func (s *SchedulerService) GetPeerTask(peerTaskId string) (peerTask *types.PeerTask, err error) {
	peerTask, _ = s.peerTaskMgr.GetPeerTask(peerTaskId)
	if peerTask == nil {
		err = errors.New("peer task not exited: " + peerTaskId)
	}
	return
}

func (s *SchedulerService) AddPeerTask(pid string, task *types.Task, host *types.Host) (ret *types.PeerTask, err error) {
	ret = s.peerTaskMgr.AddPeerTask(pid, task, host)
	host.AddPeerTask(ret)
	return
}

func (s *SchedulerService) DeletePeerTask(peerTaskId string) (err error) {
	peerTask, err := s.GetPeerTask(peerTaskId)
	if err != nil {
		return
	}
	// delete from manager
	s.peerTaskMgr.DeletePeerTask(peerTaskId)
	// delete from host
	peerTask.Host.DeletePeerTask(peerTaskId)
	// delete from piece lazy
	peerTask.SetDown()
	return
}
