/*
 *     Copyright 2024 The Dragonfly Authors
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

//go:generate mockgen -destination task_manager_mock.go -source task_manager.go -package standard

package persistentcache

import "github.com/redis/go-redis/v9"

// TaskManager is the interface used for persistent cache task manager.
type TaskManager interface {
	// Load returns persistent cache task for a key.
	Load(string) (*Task, bool)

	// Store sets persistent cache task.
	Store(*Task)

	// LoadOrStore returns persistent cache task the key if present.
	// Otherwise, it stores and returns the given persistent cache task.
	// The loaded result is true if the persistent cache task was loaded,
	// false if stored.
	LoadOrStore(*Task) (*Task, bool)

	// Delete deletes persistent cache task for a key.
	Delete(string)

	// Range calls f sequentially for each key and value present in the map.
	// If f returns false, range stops the iteration.
	Range(f func(any, any) bool)
}

// taskManager contains content for persistent cache task manager.
type taskManager struct {
	// Redis universal client interface.
	rdb redis.UniversalClient
}

// New persistent cache task manager interface.
func newTaskManager(rdb redis.UniversalClient) TaskManager {
	return &taskManager{rdb}
}

// Load returns task for a key.
func (t *taskManager) Load(key string) (*Task, bool) {
	rawTask, loaded := t.Map.Load(key)
	if !loaded {
		return nil, false
	}

	return rawTask.(*Task), loaded
}

// Store sets task.
func (t *taskManager) Store(task *Task) {
	t.Map.Store(task.ID, task)
}

// LoadOrStore returns task the key if present.
// Otherwise, it stores and returns the given task.
// The loaded result is true if the task was loaded, false if stored.
func (t *taskManager) LoadOrStore(task *Task) (*Task, bool) {
	rawTask, loaded := t.Map.LoadOrStore(task.ID, task)
	return rawTask.(*Task), loaded
}

// Delete deletes task for a key.
func (t *taskManager) Delete(key string) {
	t.Map.Delete(key)
}

// Range calls f sequentially for each key and value present in the map.
// If f returns false, range stops the iteration.
func (t *taskManager) Range(f func(key, value any) bool) {
	t.Map.Range(f)
}
