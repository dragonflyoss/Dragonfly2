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
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/types"
	"errors"
)

func (s *SchedulerService) GenerateTaskId(url string, filter string, meta *base.UrlMeta, bizId string, peerId string) (taskId string) {
	if config.GetConfig().Scheduler.ABTest {
		return idgen.GenerateTwinsTaskId(url, filter, meta, bizId, peerId)
	}
	return idgen.GenerateTaskId(url, filter, meta, bizId)
}

func (s *SchedulerService) GetTask(taskId string) (task *types.Task, err error) {
	task, _ = s.taskMgr.GetTask(taskId)
	if task == nil {
		err = errors.New("peer task not exited: " + taskId)
	}
	return
}

func (s *SchedulerService) AddTask(task *types.Task) (ret *types.Task, err error) {
	ret, added := s.taskMgr.AddTask(task)
	if added {
		err = s.cdnMgr.TriggerTask(ret)
	}
	return
}
