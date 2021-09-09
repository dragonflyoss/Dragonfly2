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

//go:generate mockgen -destination ./mock/mock_task_mgr.go -package mock d7y.io/dragonfly/v2/scheduler/supervisor TaskMgr

package supervisor

type TaskMgr interface {
	Add(task *Task)

	Get(taskID string) (task *Task, ok bool)

	Delete(taskID string)

	GetOrAdd(task *Task) (actual *Task, loaded bool)
}
