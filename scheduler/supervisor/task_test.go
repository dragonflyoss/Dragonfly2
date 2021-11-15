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

package supervisor_test

import (
	"strconv"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/supervisor"
	"d7y.io/dragonfly/v2/scheduler/supervisor/mocks"
)

func TestTask_New(t *testing.T) {
	tests := []struct {
		name   string
		task   *supervisor.Task
		expect func(t *testing.T, task *supervisor.Task)
	}{
		{
			name: "create by normal config",
			task: supervisor.NewTask("main", "127.0.0.1", &base.UrlMeta{}),
			expect: func(t *testing.T, task *supervisor.Task) {
				assert := assert.New(t)
				assert.Equal("main", task.ID)
			},
		},
		{
			name: "create by special symbol",
			task: supervisor.NewTask("\x07\b%$!！\x7F✌ (>‿<)✌", "d7y.io/dragonfly", &base.UrlMeta{Tag: "d7y-test"}),
			expect: func(t *testing.T, task *supervisor.Task) {
				assert := assert.New(t)
				assert.Equal("\x07\b%$!！\x7F✌ (>‿<)✌", task.ID)
			},
		},
		{
			name: "create by http url",
			task: supervisor.NewTask("task", "http://370.moe/", &base.UrlMeta{}),
			expect: func(t *testing.T, task *supervisor.Task) {
				assert := assert.New(t)
				assert.Equal("task", task.ID)
			},
		},
		{
			name: "create by normal config",
			task: supervisor.NewTask("task", "android://370.moe", &base.UrlMeta{}),
			expect: func(t *testing.T, task *supervisor.Task) {
				assert := assert.New(t)
				assert.Equal("task", task.ID)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, tc.task)
		})
	}
}

func TestTask_Status(t *testing.T) {
	tests := []struct {
		name       string
		status     supervisor.TaskStatus
		statusName string
		judgeArray []bool
		expect     func(t *testing.T, task *supervisor.Task, status supervisor.TaskStatus, statusName string, judgeArray []bool)
	}{
		{
			name:       "status Waiting",
			status:     supervisor.TaskStatusWaiting,
			statusName: "Waiting",
			judgeArray: []bool{false, false, true, false, false},
			expect: func(t *testing.T, task *supervisor.Task, status supervisor.TaskStatus, statusName string, judgeArray []bool) {
				assert := assert.New(t)
				assert.Equal(supervisor.TaskStatus.String(status), statusName)
				assert.Equal(task.GetStatus(), status)

				statutusJudgeArray := []bool{
					task.IsSuccess(), task.CanSchedule(),
					task.IsWaiting(), task.IsHealth(), task.IsFail(),
				}
				assert.Equal(statutusJudgeArray, judgeArray)
			},
		},
		{
			name:       "status Running",
			status:     supervisor.TaskStatusRunning,
			statusName: "Running",
			judgeArray: []bool{false, false, false, true, false},
			expect: func(t *testing.T, task *supervisor.Task, status supervisor.TaskStatus, statusName string, judgeArray []bool) {
				assert := assert.New(t)
				assert.Equal(supervisor.TaskStatus.String(status), statusName)
				assert.Equal(task.GetStatus(), status)

				statutusJudgeArray := []bool{
					task.IsSuccess(), task.CanSchedule(),
					task.IsWaiting(), task.IsHealth(), task.IsFail(),
				}
				assert.Equal(statutusJudgeArray, judgeArray)
			},
		},
		{
			name:       "status seeding",
			status:     supervisor.TaskStatusSeeding,
			statusName: "Seeding",
			judgeArray: []bool{false, true, false, true, false},
			expect: func(t *testing.T, task *supervisor.Task, status supervisor.TaskStatus, statusName string, judgeArray []bool) {
				assert := assert.New(t)
				assert.Equal(supervisor.TaskStatus.String(status), statusName)
				assert.Equal(task.GetStatus(), status)

				statutusJudgeArray := []bool{
					task.IsSuccess(), task.CanSchedule(),
					task.IsWaiting(), task.IsHealth(), task.IsFail(),
				}
				assert.Equal(statutusJudgeArray, judgeArray)
			},
		},
		{
			name:       "status success",
			status:     supervisor.TaskStatusSuccess,
			statusName: "Success",
			judgeArray: []bool{true, true, false, true, false},
			expect: func(t *testing.T, task *supervisor.Task, status supervisor.TaskStatus, statusName string, judgeArray []bool) {
				assert := assert.New(t)
				assert.Equal(supervisor.TaskStatus.String(status), statusName)
				assert.Equal(task.GetStatus(), status)

				statutusJudgeArray := []bool{
					task.IsSuccess(), task.CanSchedule(),
					task.IsWaiting(), task.IsHealth(), task.IsFail(),
				}
				assert.Equal(statutusJudgeArray, judgeArray)
			},
		},
		{
			name:       "status zombie",
			status:     supervisor.TaskStatusZombie,
			statusName: "Zombie",
			judgeArray: []bool{false, false, false, false, false},
			expect: func(t *testing.T, task *supervisor.Task, status supervisor.TaskStatus, statusName string, judgeArray []bool) {
				assert := assert.New(t)
				assert.Equal(supervisor.TaskStatus.String(status), statusName)
				assert.Equal(task.GetStatus(), status)

				statutusJudgeArray := []bool{
					task.IsSuccess(), task.CanSchedule(),
					task.IsWaiting(), task.IsHealth(), task.IsFail(),
				}
				assert.Equal(statutusJudgeArray, judgeArray)
			},
		},
		{
			name:       "status Fail",
			status:     supervisor.TaskStatusFail,
			statusName: "Fail",
			judgeArray: []bool{false, false, false, false, true},
			expect: func(t *testing.T, task *supervisor.Task, status supervisor.TaskStatus, statusName string, judgeArray []bool) {
				assert := assert.New(t)
				assert.Equal(supervisor.TaskStatus.String(status), statusName)
				assert.Equal(task.GetStatus(), status)

				statutusJudgeArray := []bool{
					task.IsSuccess(), task.CanSchedule(),
					task.IsWaiting(), task.IsHealth(), task.IsFail(),
				}
				assert.Equal(statutusJudgeArray, judgeArray)
			},
		},
		{
			name:       "unknown",
			status:     100,
			statusName: "unknown",
			judgeArray: []bool{false, false, false, false, false},
			expect: func(t *testing.T, task *supervisor.Task, status supervisor.TaskStatus, statusName string, judgeArray []bool) {
				assert := assert.New(t)
				assert.Equal(supervisor.TaskStatus.String(status), statusName)
				assert.Equal(task.GetStatus(), status)

				statutusJudgeArray := []bool{
					task.IsSuccess(), task.CanSchedule(),
					task.IsWaiting(), task.IsHealth(), task.IsFail(),
				}
				assert.Equal(statutusJudgeArray, judgeArray)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := mockATask("task")
			task.SetStatus(tc.status)
			tc.expect(t, task, tc.status, tc.statusName, tc.judgeArray)
		})
	}
}

func TestTask_BackToSourcePeer(t *testing.T) {
	tests := []struct {
		name          string
		initialWeight int32
		add           []string
		expect        func(t *testing.T, task *supervisor.Task, add []string)
	}{
		{
			name:          "able to backsource",
			initialWeight: 4,
			add:           []string{"0", "1", "2"},
			expect: func(t *testing.T, task *supervisor.Task, add []string) {
				assert := assert.New(t)
				assert.EqualValues(task.BackToSourceWeight.Load(), 1)
				assert.True(task.CanBackToSource())
				assert.ElementsMatch(task.GetBackToSourcePeers(), add)
				for _, ID := range add {
					contain := task.ContainsBackToSourcePeer(ID)
					assert.True(contain)
				}

			},
		},
		{
			name:          "unable to backsource",
			initialWeight: -1,
			add:           []string{},
			expect: func(t *testing.T, task *supervisor.Task, add []string) {
				assert := assert.New(t)
				assert.EqualValues(task.BackToSourceWeight.Load(), -1)
				assert.False(task.CanBackToSource())

			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := mockATask("task")
			task.BackToSourceWeight.Store(tc.initialWeight)
			for _, ID := range tc.add {
				task.AddBackToSourcePeer(ID)
			}
			tc.expect(t, task, tc.add)
		})
	}
}

func TestTask_Pick(t *testing.T) {
	tests := []struct {
		name    string
		number  int
		pick    func(peer *supervisor.Peer) bool
		reverse bool
		limit   int
		answer  []string
	}{
		{
			name:   "pick three odd",
			number: 10,
			pick: func(peer *supervisor.Peer) bool {
				id, _ := strconv.Atoi(peer.ID)
				return id%2 != 0
			},
			reverse: false,
			limit:   3,
			answer:  []string{"1", "3", "5"},
		},
		{
			name:   "pick 100 odd",
			number: 10,
			pick: func(peer *supervisor.Peer) bool {
				id, _ := strconv.Atoi(peer.ID)
				return id%2 != 0
			},
			reverse: true,
			limit:   3,
			answer:  []string{"5", "7", "9"},
		},
		{
			name:   "pick all odd",
			number: 10,
			pick: func(peer *supervisor.Peer) bool {
				id, _ := strconv.Atoi(peer.ID)
				return id%2 != 0
			},
			reverse: false,
			limit:   100,
			answer:  []string{"1", "3", "5", "7", "9"},
		},
		{
			name:   "pick all",
			number: 10,
			pick: func(peer *supervisor.Peer) bool {
				return true
			},
			reverse: false,
			limit:   100,
			answer:  []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"},
		},
		{
			name:   "pick nil",
			number: 10,
			pick: func(peer *supervisor.Peer) bool {
				return false
			},
			reverse: false,
			limit:   100,
			answer:  []string{},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := mockATask("task")
			for i := 0; i < tc.number; i++ {
				index := strconv.Itoa(i)
				peer := mockAPeer(index, task)
				peer.UpdateProgress((int32)(i), i)
				task.AddPeer(peer)
			}
			var peers []*supervisor.Peer
			if tc.reverse {
				peers = task.PickReverse(tc.limit, tc.pick)
			} else {
				peers = task.Pick(tc.limit, tc.pick)
			}
			var peerIDs []string
			for _, peer := range peers {
				peerIDs = append(peerIDs, peer.ID)
			}
			assert := assert.New(t)
			assert.ElementsMatch(peerIDs, tc.answer)
		})
	}
}

func TestTaskManager_New(t *testing.T) {
	tests := []struct {
		name   string
		config *config.GCConfig
		mock   func(m *mocks.MockGCMockRecorder)
		expect func(t *testing.T, taskManager supervisor.TaskManager, err error)
	}{
		{
			name:   "simple create",
			config: config.New().Scheduler.GC,
			mock: func(m *mocks.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).AnyTimes()
			},
			expect: func(t *testing.T, taskManager supervisor.TaskManager, err error) {
				assert := assert.New(t)
				assert.NotNil(taskManager)
				assert.Nil(err)
			},
		},
		{
			name:   "gc failed",
			config: config.New().Scheduler.GC,
			mock: func(m *mocks.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(errors.New("mockError")).AnyTimes()
			},
			expect: func(t *testing.T, taskManager supervisor.TaskManager, err error) {
				assert := assert.New(t)
				assert.Nil(taskManager)
				assert.Error(err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			mockAPeerManager := mocks.NewMockPeerManager(ctl)
			mockGC := mocks.NewMockGC(ctl)
			tc.mock(mockGC.EXPECT())

			taskManager, err := supervisor.NewTaskManager(tc.config, mockGC, mockAPeerManager)
			tc.expect(t, taskManager, err)
		})
	}
}

func TestTaskManager_Get(t *testing.T) {
	tests := []struct {
		name   string
		number int
		fetch  int
		expect func(t *testing.T, task *supervisor.Task, success bool)
	}{
		{
			name:   "fetch first task",
			number: 3,
			fetch:  0,
			expect: func(t *testing.T, task *supervisor.Task, success bool) {
				assert := assert.New(t)
				assert.Equal("0", task.ID)
				assert.True(success)
			},
		},
		{
			name:   "fetch last task",
			number: 3,
			fetch:  2,
			expect: func(t *testing.T, task *supervisor.Task, success bool) {
				assert := assert.New(t)
				assert.Equal("2", task.ID)
				assert.True(success)
			},
		},
		{
			name:   "fetch not exist task",
			number: 3,
			fetch:  -1,
			expect: func(t *testing.T, task *supervisor.Task, success bool) {
				assert := assert.New(t)
				assert.Equal((*supervisor.Task)(nil), task)
				assert.False(success)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			mockHostManager := mocks.NewMockPeerManager(ctl)
			mockGC := mocks.NewMockGC(ctl)
			mockGC.EXPECT().Add(gomock.Any()).Return(nil).AnyTimes()

			cfg := config.New()
			taskManager, _ := supervisor.NewTaskManager(cfg.Scheduler.GC, mockGC, mockHostManager)
			for i := 0; i < tc.number; i++ {
				index := strconv.Itoa(i)
				task := mockATask(index)
				taskManager.Add(task)
			}
			task, success := taskManager.Get(strconv.Itoa(tc.fetch))
			tc.expect(t, task, success)
		})
	}
}

func TestTaskManager_GetOrAdd(t *testing.T) {
	tests := []struct {
		name   string
		create int
		add    int
		expect func(t *testing.T, task *supervisor.Task, success bool)
	}{
		{
			name:   "get exist task",
			create: 3,
			add:    0,
			expect: func(t *testing.T, task *supervisor.Task, success bool) {
				assert := assert.New(t)
				assert.Equal("2", task.ID)
				assert.False(success)
			},
		},
		{
			name:   "add not exist task",
			create: 3,
			add:    3,
			expect: func(t *testing.T, task *supervisor.Task, success bool) {
				assert := assert.New(t)
				assert.Equal("2", task.ID)
				assert.True(success)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			mockHostManager := mocks.NewMockPeerManager(ctl)
			mockGC := mocks.NewMockGC(ctl)
			mockGC.EXPECT().Add(gomock.Any()).Return(nil).AnyTimes()

			cfg := config.New()
			taskManager, _ := supervisor.NewTaskManager(cfg.Scheduler.GC, mockGC, mockHostManager)
			var tasks []*supervisor.Task
			for i := 0; i < tc.create; i++ {
				index := strconv.Itoa(i)
				task := mockATask(index)
				tasks = append(tasks, task)
			}
			for i := 0; i < tc.add; i++ {
				taskManager.Add(tasks[i])
			}
			task, success := taskManager.GetOrAdd(tasks[len(tasks)-1])
			tc.expect(t, task, success)
		})
	}
}

func TestTaskManager_Delete(t *testing.T) {
	tests := []struct {
		name   string
		number int
		delete int
		fetch  int
		expect func(t *testing.T, task *supervisor.Task, success bool)
	}{
		{
			name:   "delete exist task",
			number: 1,
			delete: 0,
			fetch:  0,
			expect: func(t *testing.T, task *supervisor.Task, success bool) {
				assert := assert.New(t)
				assert.Nil(task)
				assert.False(success)
			},
		},
		{
			name:   "delete not exist task",
			number: 1,
			delete: 100,
			fetch:  0,
			expect: func(t *testing.T, task *supervisor.Task, success bool) {
				assert := assert.New(t)
				assert.NotNil(task)
				assert.True(success)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			mockAPeerManager := mocks.NewMockPeerManager(ctl)
			mockGC := mocks.NewMockGC(ctl)
			mockGC.EXPECT().Add(gomock.Any()).Return(nil).AnyTimes()

			cfg := config.New()
			taskManager, _ := supervisor.NewTaskManager(cfg.Scheduler.GC, mockGC, mockAPeerManager)
			for i := 0; i < tc.number; i++ {
				index := strconv.Itoa(i)
				task := mockATask(index)
				taskManager.Add(task)
			}
			taskManager.Delete(strconv.Itoa(tc.delete))
			task, success := taskManager.Get(strconv.Itoa(tc.fetch))

			tc.expect(t, task, success)
		})
	}
}

func mockATask(ID string) *supervisor.Task {
	urlMeta := &base.UrlMeta{
		Tag: "d7y-test",
	}
	return supervisor.NewTask(ID, "d7y.io/dragonfly", urlMeta)
}
