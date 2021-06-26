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

package task

import (
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/daemon/host"
	"d7y.io/dragonfly/v2/scheduler/daemon/peer"
	"d7y.io/dragonfly/v2/scheduler/manager"
	"d7y.io/dragonfly/v2/scheduler/types"
)

type TaskManager struct {
	data        sync.Map
	gcDelayTime time.Duration

	PeerTask *peer.PeerTask
}

func newTaskManager(cfg *config.Config, hostManager *host.HostManager) *TaskManager {
	delay := time.Hour * 48
	// TODO(Gaius) TaskDelay use the time.Duration
	if cfg.GC.TaskDelay > 0 {
		delay = time.Duration(cfg.GC.TaskDelay) * time.Millisecond
	}

	tm := &TaskManager{
		gcDelayTime: delay,
	}

	peerTask := manager.newPeerTask(cfg, tm, hostManager)
	tm.PeerTask = peerTask

	go tm.gcWorkingLoop()
	return tm
}

func (m *TaskManager) Set(k string, task *types.Task) {
	m.set(k, task)
}

func (m *TaskManager) set(k string, task *types.Task) {
	task.InitProps()
	m.data[k] = task
}

func (m *TaskManager) Add(k string, task *types.Task) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if _, found := m.get(k); found {
		return fmt.Errorf("Task %s already exists", k)
	}
	m.set(k, task)
	return nil
}

func (m *TaskManager) Get(taskID string) (*types.Task, bool) {
	item, ok := m.data.Load(taskID)
	return item.(*types.Task), ok
}

func (m *TaskManager) Delete(taskID string) {
	m.data.Delete(taskID)
}

func (m *TaskManager) Touch(k string) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.touch(k)
	return
}

func (m *TaskManager) touch(k string) {
	if t, ok := m.data[k]; ok {
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
			m.data.Range(func(taskID, task interface{}) bool {
				if time.Now().After(task.(types.Task).LastActive.Add(m.gcDelayTime)) {
					m.data.Delete(taskID.(string))
				}
				return true
			})

			m.PeerTask.ClearPeerTask()
		}()
	}
}
