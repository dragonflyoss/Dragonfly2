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

	"d7y.io/dragonfly/v2/pkg/gc/mocks"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestGCAdd(t *testing.T) {
	tests := []struct {
		name   string
		task   Task
		expect func(t *testing.T, err error)
	}{
		{
			name: "new GC instance succeeded",
			task: Task{
				ID:       "gc",
				Interval: 2 * time.Second,
				Timeout:  1 * time.Second,
				RunGC:    func() error { return nil },
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
				RunGC:    func() error { return nil },
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
				RunGC:    func() error { return nil },
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
				RunGC:    func() error { return nil },
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "Timeout value needs to be less than the Interval value")
			},
		},
		{
			name: "RunGC is empty",
			task: Task{
				ID:       "gc",
				Interval: 2 * time.Second,
				Timeout:  1 * time.Second,
				RunGC:    nil,
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "empty RunGC is not specified")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			mockLogger := mocks.NewMockLogger(ctl)

			gc := New([]Option{
				WithLogger(mockLogger),
			}...)

			tc.expect(t, gc.Add(tc.task))
		})
	}
}

func TestGCRun(t *testing.T) {
	tests := []struct {
		name string
		task Task
		run  func(gc GC, id string, ml *mocks.MockLogger, t *testing.T)
	}{
		{
			name: "run task succeeded",
			task: Task{
				ID:       "foo",
				Interval: 2 * time.Hour,
				Timeout:  1 * time.Hour,
				RunGC:    func() error { return nil },
			},
			run: func(gc GC, id string, ml *mocks.MockLogger, t *testing.T) {
				var wg sync.WaitGroup
				wg.Add(3)
				defer wg.Wait()

				ml.EXPECT().Infof(gomock.Any(), gomock.Eq("foo")).Do(func(template interface{}, args ...interface{}) { wg.Done() }).Times(3)
				gc.Run(id)
			},
		},
		{
			name: "task run GC failed",
			task: Task{
				ID:       "foo",
				Interval: 2 * time.Hour,
				Timeout:  1 * time.Hour,
				RunGC:    func() error { return errors.New("bar") },
			},
			run: func(gc GC, id string, ml *mocks.MockLogger, t *testing.T) {
				var wg sync.WaitGroup
				wg.Add(4)
				defer wg.Wait()

				gomock.InOrder(
					ml.EXPECT().Infof(gomock.Any(), gomock.Eq("foo")).Do(func(template interface{}, args ...interface{}) { wg.Done() }).Times(1),
					ml.EXPECT().Errorf(gomock.Any(), gomock.Eq("foo"), gomock.Eq(errors.New("bar"))).Do(func(template interface{}, args ...interface{}) { wg.Done() }).Times(1),
					ml.EXPECT().Infof(gomock.Any(), gomock.Eq("foo")).Do(func(template interface{}, args ...interface{}) { wg.Done() }).Times(2),
				)

				gc.Run(id)
			},
		},
		{
			name: "task load wrong key",
			task: Task{
				ID:       "foo",
				Interval: 2 * time.Hour,
				Timeout:  1 * time.Hour,
				RunGC:    func() error { return nil },
			},
			run: func(gc GC, id string, ml *mocks.MockLogger, t *testing.T) {
				assert := assert.New(t)
				assert.EqualError(gc.Run("bar"), "can not find the task")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			mockLogger := mocks.NewMockLogger(ctl)

			gc := New([]Option{
				WithLogger(mockLogger),
			}...)

			if err := gc.Add(tc.task); err != nil {
				t.Fatal(err)
			}

			tc.run(gc, tc.task.ID, mockLogger, t)
		})
	}
}

func TestGCRunAll(t *testing.T) {
	tests := []struct {
		name  string
		task1 Task
		task2 Task
		run   func(gc GC, ml *mocks.MockLogger)
	}{
		{
			name: "run task succeeded",
			task1: Task{
				ID:       "foo",
				Interval: 2 * time.Hour,
				Timeout:  1 * time.Hour,
				RunGC:    func() error { return nil },
			},
			task2: Task{
				ID:       "bar",
				Interval: 2 * time.Hour,
				Timeout:  1 * time.Hour,
				RunGC:    func() error { return nil },
			},
			run: func(gc GC, ml *mocks.MockLogger) {
				var wg sync.WaitGroup
				wg.Add(6)
				defer wg.Wait()

				ml.EXPECT().Infof(gomock.Any(), gomock.Eq("foo")).Do(func(template interface{}, args ...interface{}) { wg.Done() }).Times(3)
				ml.EXPECT().Infof(gomock.Any(), gomock.Eq("bar")).Do(func(template interface{}, args ...interface{}) { wg.Done() }).Times(3)
				gc.RunAll()
			},
		},
		{
			name: "task run GC failed",
			task1: Task{
				ID:       "foo",
				Interval: 2 * time.Hour,
				Timeout:  1 * time.Hour,
				RunGC:    func() error { return errors.New("baz") },
			},
			task2: Task{
				ID:       "bar",
				Interval: 2 * time.Hour,
				Timeout:  1 * time.Hour,
				RunGC:    func() error { return errors.New("baz") },
			},
			run: func(gc GC, ml *mocks.MockLogger) {
				var wg sync.WaitGroup
				wg.Add(8)
				defer wg.Wait()

				gomock.InOrder(
					ml.EXPECT().Infof(gomock.Any(), gomock.Eq("foo")).Do(func(template interface{}, args ...interface{}) { wg.Done() }).Times(1),
					ml.EXPECT().Errorf(gomock.Any(), gomock.Eq("foo"), gomock.Eq(errors.New("baz"))).Do(func(template interface{}, args ...interface{}) { wg.Done() }).Times(1),
					ml.EXPECT().Infof(gomock.Any(), gomock.Eq("foo")).Do(func(template interface{}, args ...interface{}) { wg.Done() }).Times(2),
				)

				gomock.InOrder(
					ml.EXPECT().Infof(gomock.Any(), gomock.Eq("bar")).Do(func(template interface{}, args ...interface{}) { wg.Done() }).Times(1),
					ml.EXPECT().Errorf(gomock.Any(), gomock.Eq("bar"), gomock.Eq(errors.New("baz"))).Do(func(template interface{}, args ...interface{}) { wg.Done() }).Times(1),
					ml.EXPECT().Infof(gomock.Any(), gomock.Eq("bar")).Do(func(template interface{}, args ...interface{}) { wg.Done() }).Times(2),
				)

				gc.RunAll()
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			mockLogger := mocks.NewMockLogger(ctl)

			gc := New([]Option{
				WithLogger(mockLogger),
			}...)

			if err := gc.Add(tc.task1); err != nil {
				t.Fatal(err)
			}

			if err := gc.Add(tc.task2); err != nil {
				t.Fatal(err)
			}

			tc.run(gc, mockLogger)
		})
	}
}

func TestGCServe(t *testing.T) {
	ctl := gomock.NewController(t)
	mockLogger := mocks.NewMockLogger(ctl)

	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()

	gc := New([]Option{
		WithLogger(mockLogger),
	}...)
	if err := gc.Add(Task{
		ID:       "foo",
		Interval: 2 * time.Hour,
		Timeout:  1 * time.Hour,
		RunGC:    func() error { return nil },
	}); err != nil {
		t.Fatal(err)
	}

	mockLogger.EXPECT().Infof(gomock.Any(), gomock.Eq("foo")).Do(func(template string, args ...interface{}) {
		wg.Done()
	}).Times(1)

	gc.Serve()
	gc.Stop()
}
