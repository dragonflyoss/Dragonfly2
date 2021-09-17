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

package supervisor

import (
	"sync"
	"time"

	"d7y.io/dragonfly/v2/pkg/gc"
	"d7y.io/dragonfly/v2/scheduler/config"
)

const (
	TaskGCID = "task"
)

type TaskManager interface {
	Add(*Task)

	Get(string) (*Task, bool)

	Delete(string)

	GetOrAdd(*Task) (*Task, bool)
}

type taskManager struct {
	peerManager PeerManager
	gcTicker    *time.Ticker
	taskTTL     time.Duration
	taskTTI     time.Duration
	tasks       *sync.Map
}

func NewTaskManager(cfg *config.GCConfig, gcManager gc.GC, peerManager PeerManager) TaskManager {
	m := &taskManager{
		peerManager: peerManager,
		gcTicker:    time.NewTicker(cfg.TaskGCInterval),
		taskTTL:     cfg.TaskTTL,
		taskTTI:     cfg.TaskTTI,
		tasks:       &sync.Map{},
	}

	gcManager.Add(gc.Task{
		ID:       TaskGCID,
		Interval: cfg.PeerGCInterval,
		Timeout:  cfg.PeerGCInterval,
		RunGC:    m.runGC,
	})

	go m.runGC()
	return m
}

func (m *taskManager) Delete(id string) {
	m.tasks.Delete(id)
}

func (m *taskManager) Add(task *Task) {
	m.tasks.Store(task.ID, task)
}

func (m *taskManager) Get(id string) (*Task, bool) {
	task, ok := m.tasks.Load(id)
	return task.(*Task), ok
}

func (m *taskManager) GetOrAdd(t *Task) (*Task, bool) {
	task, ok := m.tasks.LoadOrStore(t.ID, t)
	return task.(*Task), ok
}

func (m *taskManager) runGC() error {
	m.tasks.Range(func(key, value interface{}) bool {
		taskID := key.(string)
		task := value.(*Task)
		elapsed := time.Since(task.GetLastAccessAt())
		if elapsed > m.taskTTI && task.IsSuccess() {
			task.Log().Info("elapsed larger than taskTTI, task status become zombie")
			task.SetStatus(TaskStatusZombie)
		}

		if task.GetPeers().Size() == 0 {
			task.Log().Info("peers is empty, task status become waiting")
			task.SetStatus(TaskStatusWaiting)
		}

		if elapsed > m.taskTTL {
			// TODO lock
			peers := m.peerManager.GetPeersByTask(taskID)
			for _, peer := range peers {
				task.Log().Infof("delete peer %s because task is time to leave", peer.ID)
				m.peerManager.Delete(peer.ID)
			}
			task.Log().Info("delete task because elapsed larger than task TTL")
			m.Delete(taskID)
		}
		return true
	})
	return nil
}
