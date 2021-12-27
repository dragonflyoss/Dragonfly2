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
	"sync"
	"time"

	pkggc "d7y.io/dragonfly/v2/pkg/gc"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/entity"
)

const (
	// GC task id
	GCTaskID = "task"
)

type Task interface {
	// Load return task entity for a key
	Load(string) (*entity.Task, bool)

	// Store set task entity
	Store(*entity.Task)

	// LoadOrStore returns task entity the key if present.
	// Otherwise, it stores and returns the given task entity.
	// The loaded result is true if the task entity was loaded, false if stored.
	LoadOrStore(*entity.Task) (*entity.Task, bool)

	// Delete deletes task entity for a key
	Delete(string)
}

type task struct {
	// Task sync map
	*sync.Map

	// Task time to live
	ttl time.Duration
}

// New task interface
func newTask(cfg *config.GCConfig, gc pkggc.GC) (Task, error) {
	t := &task{
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

func (t *task) Load(key string) (*entity.Task, bool) {
	rawTask, ok := t.Map.Load(key)
	if !ok {
		return nil, false
	}

	return rawTask.(*entity.Task), ok
}

func (t *task) Store(task *entity.Task) {
	t.Map.Store(task.ID, task)
}

func (t *task) LoadOrStore(task *entity.Task) (*entity.Task, bool) {
	rawTask, loaded := t.Map.LoadOrStore(task.ID, task)
	return rawTask.(*entity.Task), loaded
}

func (t *task) Delete(key string) {
	t.Map.Delete(key)
}

func (t *task) RunGC() error {
	t.Map.Range(func(_, value interface{}) bool {
		task := value.(*entity.Task)
		elapsed := time.Since(task.UpdateAt.Load())

		if elapsed > t.ttl && task.LenPeers() == 0 {
			task.Log.Info("task gc succeeded")
			t.Delete(task.ID)
		}

		return true
	})

	return nil
}
