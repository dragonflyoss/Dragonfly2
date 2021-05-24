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
	"testing"

	"d7y.io/dragonfly/v2/scheduler/types"
	"github.com/stretchr/testify/assert"
)

func TestTaskManager_Set(t *testing.T) {
	tests := []struct {
		name        string
		taskManager *TaskManager
		task        *types.Task
		key         string
		expect      func(t *testing.T, d interface{})
	}{
		{
			name: "set foo task",
			taskManager: &TaskManager{
				lock: new(sync.RWMutex),
				data: make(map[string]*types.Task),
			},
			key: "foo",
			task: &types.Task{
				TaskId: "bar",
			},
			expect: func(t *testing.T, d interface{}) {
				assert := assert.New(t)
				assert.NotEmpty(d)
			},
		},
		{
			name: "set empty task",
			taskManager: &TaskManager{
				lock: new(sync.RWMutex),
				data: make(map[string]*types.Task),
			},
			key:  "foo",
			task: &types.Task{},
			expect: func(t *testing.T, d interface{}) {
				assert := assert.New(t)
				assert.NotEmpty(d)
			},
		},
		{
			name: "set empty key",
			taskManager: &TaskManager{
				lock: new(sync.RWMutex),
				data: make(map[string]*types.Task),
			},
			key: "",
			task: &types.Task{
				TaskId: "bar",
			},
			expect: func(t *testing.T, d interface{}) {
				assert := assert.New(t)
				assert.NotEmpty(d)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.taskManager.Set(tc.key, tc.task)
			tc.expect(t, tc.taskManager.data[tc.key])
		})
	}
}

func TestTaskManager_Add(t *testing.T) {
	tests := []struct {
		name        string
		taskManager *TaskManager
		task        *types.Task
		key         string
		expect      func(t *testing.T, d interface{}, err error)
	}{
		{
			name: "add foo task",
			taskManager: &TaskManager{
				lock: new(sync.RWMutex),
				data: make(map[string]*types.Task),
			},
			key: "foo",
			task: &types.Task{
				TaskId: "bar",
			},
			expect: func(t *testing.T, d interface{}, err error) {
				assert := assert.New(t)
				assert.NotEmpty(d)
			},
		},
		{
			name: "add empty task",
			taskManager: &TaskManager{
				lock: new(sync.RWMutex),
				data: make(map[string]*types.Task),
			},
			key:  "foo",
			task: &types.Task{},
			expect: func(t *testing.T, d interface{}, err error) {
				assert := assert.New(t)
				assert.NotEmpty(d)
			},
		},
		{
			name: "add empty key",
			taskManager: &TaskManager{
				lock: new(sync.RWMutex),
				data: make(map[string]*types.Task),
			},
			key: "",
			task: &types.Task{
				TaskId: "bar",
			},
			expect: func(t *testing.T, d interface{}, err error) {
				assert := assert.New(t)
				assert.NotEmpty(d)
			},
		},
		{
			name: "key already exists",
			taskManager: &TaskManager{
				lock: new(sync.RWMutex),
				data: map[string]*types.Task{"foo": nil},
			},
			key: "foo",
			task: &types.Task{
				TaskId: "bar",
			},
			expect: func(t *testing.T, d interface{}, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "Task foo already exists")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.taskManager.Add(tc.key, tc.task)
			tc.expect(t, tc.taskManager.data[tc.key], err)
		})
	}
}

func TestTaskManager_Get(t *testing.T) {
	mockTask := &types.Task{
		TaskId: "bar",
	}

	tests := []struct {
		name        string
		taskManager *TaskManager
		key         string
		expect      func(t *testing.T, task *types.Task, found bool)
	}{
		{
			name: "get existing task",
			taskManager: &TaskManager{
				lock: new(sync.RWMutex),
				data: map[string]*types.Task{"foo": mockTask},
			},
			key: "foo",
			expect: func(t *testing.T, task *types.Task, found bool) {
				assert := assert.New(t)
				assert.Equal(true, found)
				assert.Equal("bar", task.TaskId)
			},
		},
		{
			name: "get non-existent task",
			taskManager: &TaskManager{
				lock: new(sync.RWMutex),
				data: make(map[string]*types.Task),
			},
			key: "foo",
			expect: func(t *testing.T, task *types.Task, found bool) {
				assert := assert.New(t)
				assert.Equal(false, found)
				assert.Nil(task)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task, found := tc.taskManager.Get(tc.key)
			tc.expect(t, task, found)
		})
	}
}

func TestTaskManager_Delete(t *testing.T) {
	tests := []struct {
		name        string
		taskManager *TaskManager
		task        *types.Task
		key         string
		expect      func(t *testing.T, d interface{})
	}{
		{
			name: "delete existing task",
			taskManager: &TaskManager{
				lock: new(sync.RWMutex),
				data: map[string]*types.Task{"foo": nil},
			},
			key: "foo",
			expect: func(t *testing.T, d interface{}) {
				assert := assert.New(t)
				assert.Equal(d, false)
			},
		},
		{
			name: "delete non-existent task",
			taskManager: &TaskManager{
				lock: new(sync.RWMutex),
				data: make(map[string]*types.Task),
			},
			key: "foo",
			expect: func(t *testing.T, d interface{}) {
				assert := assert.New(t)
				assert.Equal(d, false)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.taskManager.Delete(tc.key)
			_, ok := tc.taskManager.Get(tc.key)
			tc.expect(t, ok)
		})
	}
}

func TestTaskManager_Touch(t *testing.T) {
	mockTask := &types.Task{
		TaskId: "bar",
	}

	tests := []struct {
		name        string
		taskManager *TaskManager
		task        *types.Task
		key         string
		expect      func(t *testing.T, task *types.Task, found bool)
	}{
		{
			name: "touch existing task",
			taskManager: &TaskManager{
				lock: new(sync.RWMutex),
				data: map[string]*types.Task{"foo": mockTask},
			},
			key: "foo",
			expect: func(t *testing.T, task *types.Task, found bool) {
				assert := assert.New(t)
				assert.Equal(found, true)
				assert.NotEmpty(task.LastActive)
			},
		},
		{
			name: "touch non-existent task",
			taskManager: &TaskManager{
				lock: new(sync.RWMutex),
				data: make(map[string]*types.Task),
			},
			key: "foo",
			expect: func(t *testing.T, task *types.Task, found bool) {
				assert := assert.New(t)
				assert.Equal(found, false)
				assert.Nil(task)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.taskManager.Touch(tc.key)
			task, found := tc.taskManager.Get(tc.key)
			tc.expect(t, task, found)
		})
	}
}
