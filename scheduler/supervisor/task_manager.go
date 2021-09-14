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

	"d7y.io/dragonfly/v2/scheduler/config"
)

type TaskManager interface {
	Add(task *Task)

	Get(taskID string) (task *Task, ok bool)

	Delete(taskID string)

	GetOrAdd(task *Task) (actual *Task, loaded bool)
}

type taskManager struct {
	peerManager              PeerManager
	cleanupExpiredTaskTicker *time.Ticker
	taskTTL                  time.Duration
	taskTTI                  time.Duration
	taskMap                  sync.Map
}

func NewTaskManager(cfg *config.GCConfig, peerManager PeerManager) TaskManager {
	m := &taskManager{
		peerManager:              peerManager,
		cleanupExpiredTaskTicker: time.NewTicker(cfg.TaskGCInterval),
		taskTTL:                  cfg.TaskTTL,
		taskTTI:                  cfg.TaskTTI,
	}
	go m.cleanupTasks()
	return m
}

func (m *taskManager) Delete(taskID string) {
	m.taskMap.Delete(taskID)
}

func (m *taskManager) Add(task *Task) {
	m.taskMap.Store(task.TaskID, task)
}

func (m *taskManager) Get(taskID string) (task *Task, ok bool) {
	item, ok := m.taskMap.Load(taskID)
	if !ok {
		return nil, false
	}
	return item.(*Task), true
}

func (m *taskManager) GetOrAdd(task *Task) (actual *Task, loaded bool) {
	item, loaded := m.taskMap.LoadOrStore(task.TaskID, task)
	if loaded {
		return item.(*Task), true
	}
	return task, false
}

func (m *taskManager) cleanupTasks() {
	for range m.cleanupExpiredTaskTicker.C {
		m.taskMap.Range(func(key, value interface{}) bool {
			taskID := key.(string)
			task := value.(*Task)
			elapse := time.Since(task.GetLastAccessTime())
			if elapse > m.taskTTI && task.IsSuccess() {
				task.Log().Info("elapse larger than taskTTI, task status become zombie")
				task.SetStatus(TaskStatusZombie)
			}
			if task.ListPeers().Size() == 0 {
				task.Log().Info("peers is empty, task status become waiting")
				task.SetStatus(TaskStatusWaiting)
			}
			if elapse > m.taskTTL {
				// TODO lock
				peers := m.peerManager.ListPeersByTask(taskID)
				for _, peer := range peers {
					task.Log().Infof("delete peer %s because task is time to leave", peer.ID)
					m.peerManager.Delete(peer.ID)
				}
				task.Log().Info("delete task because elapse larger than task TTL")
				m.Delete(taskID)
			}
			return true
		})
	}
}
