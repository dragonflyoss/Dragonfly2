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

package gc

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func TestGC_Add(t *testing.T) {
	tests := []struct {
		name   string
		task   Task
		expect func(t *testing.T, err error)
	}{
		{
			name: "new GC",
			task: Task{
				ID:       "gc",
				Interval: 2 * time.Second,
				Timeout:  1 * time.Second,
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
		{
			name: "add GC task without interval",
			task: Task{
				ID:       "gc",
				Interval: 0,
				Timeout:  1 * time.Second,
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "Interval value is greater than 0")
			},
		},
		{
			name: "add GC task without timeout",
			task: Task{
				ID:       "gc",
				Interval: 2 * time.Second,
				Timeout:  0,
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "Timeout value is greater than 0")
			},
		},
		{
			name: "timeout is greater than interval",
			task: Task{
				ID:       "gc",
				Interval: 1 * time.Second,
				Timeout:  2 * time.Second,
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "Timeout value needs to be less than the Interval value")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			mockLogger := NewMockLogger(ctl)
			mockRunner := NewMockRunner(ctl)

			gc := New(WithLogger(mockLogger))

			tc.expect(t, gc.Add(Task{
				ID:       tc.task.ID,
				Interval: tc.task.Interval,
				Timeout:  tc.task.Timeout,
				Runner:   mockRunner,
			}))
		})
	}
}

func TestGC_Run(t *testing.T) {
	tests := []struct {
		name string
		task Task
		run  func(gc GC, id string, ml *MockLogger, mr *MockRunner, t *testing.T)
	}{
		{
			name: "run task",
			task: Task{
				ID:       "foo",
				Interval: 2 * time.Hour,
				Timeout:  1 * time.Hour,
			},
			run: func(gc GC, id string, ml *MockLogger, mr *MockRunner, t *testing.T) {
				var wg sync.WaitGroup
				wg.Add(3)
				defer wg.Wait()

				gomock.InOrder(
					ml.EXPECT().Infof(gomock.Any(), gomock.Eq("foo")).Do(func(template any, args ...any) { wg.Done() }).Times(1),
					mr.EXPECT().RunGC().Do(func() { wg.Done() }).Return(nil).Times(1),
					ml.EXPECT().Infof(gomock.Any(), gomock.Eq("foo")).Do(func(template any, args ...any) { wg.Done() }).Times(1),
				)

				if err := gc.Run(id); err != nil {
					t.Error(err)
				}
			},
		},
		{
			name: "task run GC failed",
			task: Task{
				ID:       "foo",
				Interval: 2 * time.Hour,
				Timeout:  1 * time.Hour,
			},
			run: func(gc GC, id string, ml *MockLogger, mr *MockRunner, t *testing.T) {
				var wg sync.WaitGroup
				wg.Add(4)
				defer wg.Wait()

				err := errors.New("bar")
				gomock.InOrder(
					ml.EXPECT().Infof(gomock.Any(), gomock.Eq("foo")).Do(func(template any, args ...any) { wg.Done() }).Times(1),
					mr.EXPECT().RunGC().Do(func() { wg.Done() }).Return(err).Times(1),
					ml.EXPECT().Errorf(gomock.Any(), gomock.Eq("foo"), gomock.Eq(err)).Do(func(template any, args ...any) { wg.Done() }).Times(1),
					ml.EXPECT().Infof(gomock.Any(), gomock.Eq("foo")).Do(func(template any, args ...any) { wg.Done() }).Times(1),
				)

				if err := gc.Run(id); err != nil {
					t.Error(err)
				}
			},
		},
		{
			name: "task load wrong key",
			task: Task{
				ID:       "foo",
				Interval: 2 * time.Hour,
				Timeout:  1 * time.Hour,
			},
			run: func(gc GC, id string, ml *MockLogger, mr *MockRunner, t *testing.T) {
				assert := assert.New(t)
				assert.EqualError(gc.Run("bar"), "can not find task bar")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			mockLogger := NewMockLogger(ctl)
			mockRunner := NewMockRunner(ctl)

			gc := New(WithLogger(mockLogger))
			if err := gc.Add(Task{
				ID:       tc.task.ID,
				Interval: tc.task.Interval,
				Timeout:  tc.task.Timeout,
				Runner:   mockRunner,
			}); err != nil {
				t.Fatal(err)
			}

			tc.run(gc, tc.task.ID, mockLogger, mockRunner, t)
		})
	}
}

func TestGC_RunAll(t *testing.T) {
	tests := []struct {
		name  string
		task1 Task
		task2 Task
		run   func(gc GC, ml *MockLogger, mr *MockRunner)
	}{
		{
			name: "run task",
			task1: Task{
				ID:       "foo",
				Interval: 2 * time.Hour,
				Timeout:  1 * time.Hour,
			},
			task2: Task{
				ID:       "bar",
				Interval: 2 * time.Hour,
				Timeout:  1 * time.Hour,
			},
			run: func(gc GC, ml *MockLogger, mr *MockRunner) {
				var wg sync.WaitGroup
				wg.Add(3)
				defer wg.Wait()

				gomock.InOrder(
					ml.EXPECT().Infof(gomock.Any(), gomock.Eq("foo")).Do(func(template any, args ...any) { wg.Done() }).Times(1),
					mr.EXPECT().RunGC().Do(func() { wg.Done() }).Return(nil).Times(1),
					ml.EXPECT().Infof(gomock.Any(), gomock.Eq("foo")).Do(func(template any, args ...any) { wg.Done() }).Times(1),
				)

				gc.RunAll()
			},
		},
		{
			name: "task run GC failed",
			task1: Task{
				ID:       "foo",
				Interval: 2 * time.Hour,
				Timeout:  1 * time.Hour,
			},
			task2: Task{
				ID:       "bar",
				Interval: 2 * time.Hour,
				Timeout:  1 * time.Hour,
			},
			run: func(gc GC, ml *MockLogger, mr *MockRunner) {
				var wg sync.WaitGroup
				wg.Add(4)
				defer wg.Wait()

				err := errors.New("baz")
				gomock.InOrder(
					ml.EXPECT().Infof(gomock.Any(), gomock.Eq("foo")).Do(func(template any, args ...any) { wg.Done() }).Times(1),
					mr.EXPECT().RunGC().Do(func() { wg.Done() }).Return(err).Times(1),
					ml.EXPECT().Errorf(gomock.Any(), gomock.Eq("foo"), gomock.Eq(err)).Do(func(template any, args ...any) { wg.Done() }).Times(1),
					ml.EXPECT().Infof(gomock.Any(), gomock.Eq("foo")).Do(func(template any, args ...any) { wg.Done() }).Times(1),
				)

				gc.RunAll()
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			mockLogger := NewMockLogger(ctl)
			mockRunner := NewMockRunner(ctl)

			gc := New(WithLogger(mockLogger))

			if err := gc.Add(Task{
				ID:       tc.task1.ID,
				Interval: tc.task1.Interval,
				Timeout:  tc.task1.Timeout,
				Runner:   mockRunner,
			}); err != nil {
				t.Fatal(err)
			}

			tc.run(gc, mockLogger, mockRunner)
		})
	}
}

func TestGC_Start(t *testing.T) {
	ctl := gomock.NewController(t)
	mockLogger := NewMockLogger(ctl)
	mockRunner := NewMockRunner(ctl)

	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()

	gc := New(WithLogger(mockLogger))
	if err := gc.Add(Task{
		ID:       "foo",
		Interval: 2 * time.Hour,
		Timeout:  1 * time.Hour,
		Runner:   mockRunner,
	}); err != nil {
		t.Fatal(err)
	}

	mockLogger.EXPECT().Infof(gomock.Any(), gomock.Eq("foo")).Do(func(template string, args ...any) {
		wg.Done()
	}).Times(1)

	gc.Start()
	gc.Stop()
}
