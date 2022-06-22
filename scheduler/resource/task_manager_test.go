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

package resource

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/pkg/gc"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/scheduler/config"
)

var (
	mockTaskGCConfig = &config.GCConfig{
		TaskGCInterval: 1 * time.Second,
		TaskTTL:        1 * time.Microsecond,
	}
)

func TestTaskManager_newTaskManager(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(m *gc.MockGCMockRecorder)
		expect func(t *testing.T, taskManager TaskManager, err error)
	}{
		{
			name: "new task manager",
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, taskManager TaskManager, err error) {
				assert := assert.New(t)
				assert.Equal(reflect.TypeOf(taskManager).Elem().Name(), "taskManager")
			},
		},
		{
			name: "new task manager failed because of gc error",
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(errors.New("foo")).Times(1)
			},
			expect: func(t *testing.T, taskManager TaskManager, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "foo")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			gc := gc.NewMockGC(ctl)
			tc.mock(gc.EXPECT())

			taskManager, err := newTaskManager(mockTaskGCConfig, gc)
			tc.expect(t, taskManager, err)
		})
	}
}

func TestTaskManager_Load(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(m *gc.MockGCMockRecorder)
		expect func(t *testing.T, taskManager TaskManager, mockTask *Task)
	}{
		{
			name: "load task",
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, taskManager TaskManager, mockTask *Task) {
				assert := assert.New(t)
				taskManager.Store(mockTask)
				task, ok := taskManager.Load(mockTask.ID)
				assert.Equal(ok, true)
				assert.Equal(task.ID, mockTask.ID)
			},
		},
		{
			name: "task does not exist",
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, taskManager TaskManager, mockTask *Task) {
				assert := assert.New(t)
				_, ok := taskManager.Load(mockTask.ID)
				assert.Equal(ok, false)
			},
		},
		{
			name: "load key is empty",
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, taskManager TaskManager, mockTask *Task) {
				assert := assert.New(t)
				mockTask.ID = ""
				taskManager.Store(mockTask)
				task, ok := taskManager.Load(mockTask.ID)
				assert.Equal(ok, true)
				assert.Equal(task.ID, mockTask.ID)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			gc := gc.NewMockGC(ctl)
			tc.mock(gc.EXPECT())

			mockTask := NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, WithBackToSourceLimit(mockTaskBackToSourceLimit))
			taskManager, err := newTaskManager(mockTaskGCConfig, gc)
			if err != nil {
				t.Fatal(err)
			}

			tc.expect(t, taskManager, mockTask)
		})
	}
}

func TestTaskManager_Store(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(m *gc.MockGCMockRecorder)
		expect func(t *testing.T, taskManager TaskManager, mockTask *Task)
	}{
		{
			name: "store task",
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, taskManager TaskManager, mockTask *Task) {
				assert := assert.New(t)
				taskManager.Store(mockTask)
				task, ok := taskManager.Load(mockTask.ID)
				assert.Equal(ok, true)
				assert.Equal(task.ID, mockTask.ID)
			},
		},
		{
			name: "store key is empty",
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, taskManager TaskManager, mockTask *Task) {
				assert := assert.New(t)
				mockTask.ID = ""
				taskManager.Store(mockTask)
				task, ok := taskManager.Load(mockTask.ID)
				assert.Equal(ok, true)
				assert.Equal(task.ID, mockTask.ID)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			gc := gc.NewMockGC(ctl)
			tc.mock(gc.EXPECT())

			mockTask := NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, WithBackToSourceLimit(mockTaskBackToSourceLimit))
			taskManager, err := newTaskManager(mockTaskGCConfig, gc)
			if err != nil {
				t.Fatal(err)
			}

			tc.expect(t, taskManager, mockTask)
		})
	}
}

func TestTaskManager_LoadOrStore(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(m *gc.MockGCMockRecorder)
		expect func(t *testing.T, taskManager TaskManager, mockTask *Task)
	}{
		{
			name: "load task exist",
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, taskManager TaskManager, mockTask *Task) {
				assert := assert.New(t)
				taskManager.Store(mockTask)
				task, ok := taskManager.LoadOrStore(mockTask)
				assert.Equal(ok, true)
				assert.Equal(task.ID, mockTask.ID)
			},
		},
		{
			name: "load task does not exist",
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, taskManager TaskManager, mockTask *Task) {
				assert := assert.New(t)
				task, ok := taskManager.LoadOrStore(mockTask)
				assert.Equal(ok, false)
				assert.Equal(task.ID, mockTask.ID)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			gc := gc.NewMockGC(ctl)
			tc.mock(gc.EXPECT())

			mockTask := NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, WithBackToSourceLimit(mockTaskBackToSourceLimit))
			taskManager, err := newTaskManager(mockTaskGCConfig, gc)
			if err != nil {
				t.Fatal(err)
			}

			tc.expect(t, taskManager, mockTask)
		})
	}
}

func TestTaskManager_Delete(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(m *gc.MockGCMockRecorder)
		expect func(t *testing.T, taskManager TaskManager, mockTask *Task)
	}{
		{
			name: "delete task",
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, taskManager TaskManager, mockTask *Task) {
				assert := assert.New(t)
				taskManager.Store(mockTask)
				taskManager.Delete(mockTask.ID)
				_, ok := taskManager.Load(mockTask.ID)
				assert.Equal(ok, false)
			},
		},
		{
			name: "delete key does not exist",
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, taskManager TaskManager, mockTask *Task) {
				assert := assert.New(t)
				mockTask.ID = ""
				taskManager.Store(mockTask)
				taskManager.Delete(mockTask.ID)
				_, ok := taskManager.Load(mockTask.ID)
				assert.Equal(ok, false)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			gc := gc.NewMockGC(ctl)
			tc.mock(gc.EXPECT())

			mockTask := NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, WithBackToSourceLimit(mockTaskBackToSourceLimit))
			taskManager, err := newTaskManager(mockTaskGCConfig, gc)
			if err != nil {
				t.Fatal(err)
			}

			tc.expect(t, taskManager, mockTask)
		})
	}
}

func TestTaskManager_RunGC(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(m *gc.MockGCMockRecorder)
		expect func(t *testing.T, taskManager TaskManager, mockTask *Task, mockPeer *Peer)
	}{
		{
			name: "task reclaimed",
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, taskManager TaskManager, mockTask *Task, mockPeer *Peer) {
				assert := assert.New(t)
				taskManager.Store(mockTask)
				err := taskManager.RunGC()
				assert.NoError(err)

				_, ok := taskManager.Load(mockTask.ID)
				assert.Equal(ok, false)
			},
		},
		{
			name: "task has peers",
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, taskManager TaskManager, mockTask *Task, mockPeer *Peer) {
				assert := assert.New(t)
				taskManager.Store(mockTask)
				mockTask.StorePeer(mockPeer)
				err := taskManager.RunGC()
				assert.NoError(err)

				task, ok := taskManager.Load(mockTask.ID)
				assert.Equal(ok, true)
				assert.Equal(task.ID, mockTask.ID)
			},
		},
		{
			name: "task state is TaskStateRunning",
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, taskManager TaskManager, mockTask *Task, mockPeer *Peer) {
				assert := assert.New(t)
				taskManager.Store(mockTask)
				mockTask.FSM.SetState(TaskStateRunning)
				err := taskManager.RunGC()
				assert.NoError(err)

				task, ok := taskManager.Load(mockTask.ID)
				assert.Equal(ok, true)
				assert.Equal(task.ID, mockTask.ID)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			gc := gc.NewMockGC(ctl)
			tc.mock(gc.EXPECT())

			mockHost := NewHost(mockRawHost)
			mockTask := NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, WithBackToSourceLimit(mockTaskBackToSourceLimit))
			mockPeer := NewPeer(mockPeerID, mockTask, mockHost)
			taskManager, err := newTaskManager(mockTaskGCConfig, gc)
			if err != nil {
				t.Fatal(err)
			}

			tc.expect(t, taskManager, mockTask, mockPeer)
		})
	}
}
