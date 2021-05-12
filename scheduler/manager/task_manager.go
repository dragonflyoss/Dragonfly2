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

package manager

import (
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/types"
)

type TaskManager struct {
	lock        *sync.RWMutex
	data        map[string]*types.Task
	gcDelayTime time.Duration

	PeerTask *PeerTask
}

func newTaskManager(cfg *config.Config, hostManager *HostManager) *TaskManager {
	delay := time.Hour * 48
	// TODO(Gaius) TaskDelay use the time.Duration
	if cfg.GC.TaskDelay > 0 {
		delay = time.Duration(cfg.GC.TaskDelay) * time.Millisecond
	}

	tm := &TaskManager{
		lock:        new(sync.RWMutex),
		data:        make(map[string]*types.Task),
		gcDelayTime: delay,
	}

	peerTask := newPeerTask(cfg, tm, hostManager)
	tm.PeerTask = peerTask

	go tm.gcWorkingLoop()
	return tm
}

func (m *TaskManager) Set(task *types.Task) *types.Task {
	m.lock.Lock()
	defer m.lock.Unlock()

	return m.set(task)
}

func (m *TaskManager) set(task *types.Task) *types.Task {
	copyTask := types.CopyTask(task)
	m.data[task.TaskId] = copyTask
	return copyTask
}

func (m *TaskManager) Add(task *types.Task) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if _, found := m.get(task.TaskId); found {
		return fmt.Errorf("Task %s already exists", task.TaskId)
	}
	m.set(task)
	return nil
}

func (m *TaskManager) Get(taskId string) (*types.Task, bool) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	item, found := m.get(taskId)
	if !found {
		return nil, false
	}
	return item, true
}

func (m *TaskManager) get(taskId string) (*types.Task, bool) {
	item, found := m.data[taskId]
	return item, found
}

func (m *TaskManager) Delete(taskId string) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.delete(taskId)
	return
}

func (m *TaskManager) delete(taskId string) {
	if _, found := m.data[taskId]; found {
		delete(m.data, taskId)
		return
	}
}

func (m *TaskManager) Touch(taskId string) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.touch(taskId)
	return
}

func (m *TaskManager) touch(taskId string) {
	if t, ok := m.data[taskId]; ok {
		t.LastActive = time.Now()
	}
}

// TODO(Gaius) Use client GC manager
func (m *TaskManager) gcWorkingLoop() {
	for {
		func() {
			time.Sleep(time.Hour)
			defer func() {
				e := recover()
				if e != nil {
					logger.Error("gcWorkingLoop", e)
					debug.PrintStack()
				}
			}()
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
					m.Delete(taskId)
				}
			}

			// clear peer task
			m.PeerTask.ClearPeerTask()
		}()
	}
}
