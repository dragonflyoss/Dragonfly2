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

	"d7y.io/dragonfly/v2/cdnsystem/daemon/cdn"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/daemon"
	"d7y.io/dragonfly/v2/scheduler/types"
)

type manager struct {
	taskMap     sync.Map
	gcDelayTime time.Duration
	peerManager daemon.PeerMgr
	cdnManager  cdn.Manager
}

func (m *manager) Add(task *types.Task) {
	panic("implement me")
}

func (m *manager) Get(taskID string) (task *types.Task, ok bool) {
	panic("implement me")
}

func (m *manager) ListTasks() {
	panic("implement me")
}

func newManager(cfg *config.Config, hostManager daemon.HostMgr) daemon.TaskMgr {
	delay := 48 * time.Hour
	if cfg.GC.TaskDelay > 0 {
		delay = cfg.GC.TaskDelay
	}

	tm := &manager{
		gcDelayTime: delay,
	}

	peerTask := manager.newPeerTask(cfg, tm, hostManager)
	tm.peerManager = peerTask

	go tm.gcWorkingLoop()
	return tm
}

func (m *manager) Add(task types.Task) {
	m.taskMap.Store(task.TaskID, task)
}

func (m *manager) PutIfAbsent() {
	m.taskMap.LoadOrStore()
}

func (m *manager) Add(k string, task *types.Task) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if _, found := m.get(k); found {
		return fmt.Errorf("Task %s already exists", k)
	}
	m.set(k, task)
	return nil
}

func (m *manager) Load(taskID string) (types.Task, bool) {
	item, ok := m.taskMap.Load(taskID)
	// update last access
	return item.(types.Task), ok
}

func (m *manager) GC(taskID string) {
	m.taskMap.Delete(taskID)
}

// TODO(Gaius) Use client GC manager
func (m *manager) gcWorkingLoop() {
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
			m.taskMap.Range(func(taskID, task interface{}) bool {
				if time.Now().After(task.(types.Task).LastAccessTime.Add(m.gcDelayTime)) {
					m.taskMap.Delete(taskID.(string))
				}
				return true
			})

			m.PeerTask.ClearPeerTask()
		}()
	}
}
