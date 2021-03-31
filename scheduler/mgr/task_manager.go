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

package mgr

import (
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/types"
	"sync"
	"time"
)

type TaskManager struct {
	lock        *sync.RWMutex
	data        map[string]*types.Task
	gcDelayTime time.Duration
}

func createTaskManager() *TaskManager {
	delay := time.Hour * 48
	if config.GetConfig().GC.TaskDelay > 0 {
		delay = time.Duration(config.GetConfig().GC.TaskDelay) * time.Millisecond
	}
	tm := &TaskManager{
		lock:        new(sync.RWMutex),
		data:        make(map[string]*types.Task),
		gcDelayTime: delay,
	}
	go tm.gcWorkingLoop()
	return tm
}

func (m *TaskManager) AddTask(task *types.Task) (*types.Task, bool) {
	m.lock.Lock()
	defer m.lock.Unlock()
	v, ok := m.data[task.TaskId]
	if ok {
		return v, false
	}

	copyTask := types.CopyTask(task)

	m.data[task.TaskId] = copyTask
	return copyTask, true
}

func (m *TaskManager) DeleteTask(taskId string) {
	m.lock.Lock()
	defer m.lock.Unlock()
	t, _ := m.data[taskId]
	if t != nil {
		logger.Infof("Task [%s] Statistic: %+v ", t.TaskId, t.Statistic.GetStatistic())
		GetPeerTaskManager().DeleteTask(t)
	}
	delete(m.data, taskId)
	return
}

func (m *TaskManager) GetTask(taskId string) (h *types.Task, ok bool) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	h, ok = m.data[taskId]
	return
}

func (m *TaskManager) TouchTask(taskId string) {
	m.lock.Lock()
	defer m.lock.Unlock()
	t, _ := m.data[taskId]
	if t != nil {
		t.LastActive = time.Now()
	}
	return
}


func (m *TaskManager) gcWorkingLoop() {
	for {
		time.Sleep(time.Hour)
		var needDeleteKeys []string
		m.lock.RLock()
		for taskId, task := range m.data {
			if task != nil && time.Now().After(task.LastActive.Add(m.gcDelayTime)) {
				needDeleteKeys = append(needDeleteKeys, taskId)
				task.Removed = true
			}
		}
		m.lock.RUnlock()

		if len(needDeleteKeys) > 0 {
			for _, taskId := range needDeleteKeys {
				m.DeleteTask(taskId)
			}
		}

		// clear peer task
		GetPeerTaskManager().ClearPeerTask()
	}
}
