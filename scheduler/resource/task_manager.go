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

//go:generate mockgen -destination task_manager_mock.go -source task_manager.go -package resource

package resource

import (
	"sync"
	"time"

	pkggc "d7y.io/dragonfly/v2/pkg/gc"
	"d7y.io/dragonfly/v2/scheduler/config"
)

const (
	// GC task id.
	GCTaskID = "task"
)

type TaskManager interface {
	// Load returns task for a key.
	Load(string) (*Task, bool)

	// Store sets task.
	Store(*Task)

	// LoadOrStore returns task the key if present.
	// Otherwise, it stores and returns the given task.
	// The loaded result is true if the task was loaded, false if stored.
	LoadOrStore(*Task) (*Task, bool)

	// Delete deletes task for a key.
	Delete(string)

	// Try to reclaim task.
	RunGC() error
}

type taskManager struct {
	// Task sync map.
	*sync.Map

	// Task time to live.
	ttl time.Duration
}

// New task manager interface.
func newTaskManager(cfg *config.GCConfig, gc pkggc.GC) (TaskManager, error) {
	t := &taskManager{
		Map: &sync.Map{},
		ttl: cfg.TaskTTL,
	}

	if err := gc.Add(pkggc.Task{
		ID:       GCTaskID,
		Interval: cfg.TaskGCInterval,
		Timeout:  cfg.TaskGCInterval,
		Runner:   t,
	}); err != nil {
		return nil, err
	}

	return t, nil
}

func (t *taskManager) Load(key string) (*Task, bool) {
	rawTask, ok := t.Map.Load(key)
	if !ok {
		return nil, false
	}

	return rawTask.(*Task), ok
}

func (t *taskManager) Store(task *Task) {
	t.Map.Store(task.ID, task)
}

func (t *taskManager) LoadOrStore(task *Task) (*Task, bool) {
	rawTask, loaded := t.Map.LoadOrStore(task.ID, task)
	return rawTask.(*Task), loaded
}

func (t *taskManager) Delete(key string) {
	t.Map.Delete(key)
}

func (t *taskManager) RunGC() error {
	t.Map.Range(func(_, value any) bool {
		task := value.(*Task)
		elapsed := time.Since(task.UpdateAt.Load())

		if elapsed > t.ttl && task.PeerCount.Load() == 0 && !task.FSM.Is(TaskStateRunning) {
			task.Log.Info("task has been reclaimed")
			t.Delete(task.ID)
		}

		return true
	})

	return nil
}
