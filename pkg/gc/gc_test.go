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

func TestGCNew(t *testing.T) {
	tests := []struct {
		name     string
		interval time.Duration
		timeout  time.Duration
		expect   func(t *testing.T, err error)
	}{
		{
			name:     "new GC instance succeeded",
			interval: 2 * time.Second,
			timeout:  1 * time.Second,
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
		{
			name:     "new GC without interval",
			interval: 0,
			timeout:  1 * time.Second,
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "interval value is greater than 0")
			},
		},
		{
			name:     "new GC without timeout",
			interval: 2 * time.Second,
			timeout:  0,
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "timeout value is greater than 0")
			},
		},
		{
			name:     "timeout is greater than interval",
			interval: 1 * time.Second,
			timeout:  2 * time.Second,
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "timeout value needs to be less than the interval value")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			mockLogger := mocks.NewMockLogger(ctl)

			_, err := New([]Option{
				WithInterval(tc.interval),
				WithTimeout(tc.timeout),
				WithLogger(mockLogger),
			}...)

			tc.expect(t, err)
		})
	}
}

func TestGCRun(t *testing.T) {
	tests := []struct {
		name     string
		taskName string
		interval time.Duration
		timeout  time.Duration
		run      func(gc GC, taskName string, ml *mocks.MockLogger, mt *mocks.MockTask, t *testing.T)
	}{
		{
			name:     "run task succeeded",
			taskName: "foo",
			interval: 2 * time.Hour,
			timeout:  1 * time.Hour,
			run: func(gc GC, taskName string, ml *mocks.MockLogger, mt *mocks.MockTask, t *testing.T) {
				var wg sync.WaitGroup
				wg.Add(3)
				defer wg.Wait()

				gomock.InOrder(
					ml.EXPECT().Infof(gomock.Any(), gomock.Eq("foo"), gomock.Eq("start")).Do(func(template interface{}, args ...interface{}) { wg.Done() }).Times(1),
					mt.EXPECT().RunGC().Do(func() { wg.Done() }).Return(nil).Times(1),
					ml.EXPECT().Infof(gomock.Any(), gomock.Eq("foo"), gomock.Eq("done")).Do(func(template interface{}, args ...interface{}) { wg.Done() }).Times(1),
				)

				gc.Add(taskName, mt)
				gc.Run(taskName)
			},
		},
		{
			name:     "task run GC failed",
			taskName: "foo",
			interval: 2 * time.Hour,
			timeout:  1 * time.Hour,
			run: func(gc GC, taskName string, ml *mocks.MockLogger, mt *mocks.MockTask, t *testing.T) {
				var wg sync.WaitGroup
				wg.Add(4)
				defer wg.Wait()

				err := errors.New("bar")
				gomock.InOrder(
					ml.EXPECT().Infof(gomock.Any(), gomock.Eq("foo"), gomock.Eq("start")).Do(func(template interface{}, args ...interface{}) { wg.Done() }).Times(1),
					mt.EXPECT().RunGC().Do(func() { wg.Done() }).Return(err).Times(1),
					ml.EXPECT().Errorf(gomock.Any(), gomock.Eq("foo"), gomock.Eq(err)).Do(func(template interface{}, args ...interface{}) { wg.Done() }).Times(1),
					ml.EXPECT().Infof(gomock.Any(), gomock.Eq("foo"), gomock.Eq("done")).Do(func(template interface{}, args ...interface{}) { wg.Done() }).Times(1),
				)

				gc.Add(taskName, mt)
				gc.Run(taskName)
			},
		},
		{
			name:     "task load wrong key",
			taskName: "foo",
			interval: 2 * time.Hour,
			timeout:  1 * time.Hour,
			run: func(gc GC, taskName string, ml *mocks.MockLogger, mt *mocks.MockTask, t *testing.T) {
				assert := assert.New(t)
				gc.Add(taskName, mt)
				err := gc.Run("bar")
				assert.EqualError(err, "can not find the task")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			mockLogger := mocks.NewMockLogger(ctl)
			mockTask := mocks.NewMockTask(ctl)

			gc, err := New([]Option{
				WithInterval(tc.interval),
				WithTimeout(tc.timeout),
				WithLogger(mockLogger),
			}...)
			if err != nil {
				t.Fatal(err)
			}
			tc.run(gc, tc.taskName, mockLogger, mockTask, t)
		})
	}
}

func TestGCRunAll(t *testing.T) {
	tests := []struct {
		name     string
		taskName string
		interval time.Duration
		timeout  time.Duration
		run      func(gc GC, taskName string, ml *mocks.MockLogger, mt *mocks.MockTask)
	}{
		{
			name:     "run task succeeded",
			taskName: "foo",
			interval: 2 * time.Hour,
			timeout:  1 * time.Hour,
			run: func(gc GC, taskName string, ml *mocks.MockLogger, mt *mocks.MockTask) {
				var wg sync.WaitGroup
				wg.Add(3)
				defer wg.Wait()

				gomock.InOrder(
					ml.EXPECT().Infof(gomock.Any(), gomock.Eq("foo"), gomock.Eq("start")).Do(func(template interface{}, args ...interface{}) { wg.Done() }).Times(1),
					mt.EXPECT().RunGC().Do(func() { wg.Done() }).Return(nil).Times(1),
					ml.EXPECT().Infof(gomock.Any(), gomock.Eq("foo"), gomock.Eq("done")).Do(func(template interface{}, args ...interface{}) { wg.Done() }).Times(1),
				)

				gc.Add(taskName, mt)
				gc.RunAll()
			},
		},
		{
			name:     "task run GC failed",
			taskName: "foo",
			interval: 2 * time.Hour,
			timeout:  1 * time.Hour,
			run: func(gc GC, taskName string, ml *mocks.MockLogger, mt *mocks.MockTask) {
				var wg sync.WaitGroup
				wg.Add(4)
				defer wg.Wait()

				err := errors.New("bar")
				gomock.InOrder(
					ml.EXPECT().Infof(gomock.Any(), gomock.Eq("foo"), gomock.Eq("start")).Do(func(template interface{}, args ...interface{}) { wg.Done() }).Times(1),
					mt.EXPECT().RunGC().Do(func() { wg.Done() }).Return(err).Times(1),
					ml.EXPECT().Errorf(gomock.Any(), gomock.Eq("foo"), gomock.Eq(err)).Do(func(template interface{}, args ...interface{}) { wg.Done() }).Times(1),
					ml.EXPECT().Infof(gomock.Any(), gomock.Eq("foo"), gomock.Eq("done")).Do(func(template interface{}, args ...interface{}) { wg.Done() }).Times(1),
				)

				gc.Add(taskName, mt)
				gc.RunAll()
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			mockLogger := mocks.NewMockLogger(ctl)
			mockTask := mocks.NewMockTask(ctl)
			gc, err := New([]Option{
				WithInterval(tc.interval),
				WithTimeout(tc.timeout),
				WithLogger(mockLogger),
			}...)
			if err != nil {
				t.Fatal(err)
			}
			tc.run(gc, tc.taskName, mockLogger, mockTask)
		})
	}
}

func TestGCServe(t *testing.T) {
	ctl := gomock.NewController(t)

	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()

	mockLogger := mocks.NewMockLogger(ctl)
	mockTask := mocks.NewMockTask(ctl)

	gc, err := New([]Option{
		WithInterval(2 * time.Hour),
		WithTimeout(1 * time.Hour),
		WithLogger(mockLogger),
	}...)
	if err != nil {
		t.Fatal(err)
	}

	mockLogger.EXPECT().Infof(gomock.Eq("GC stop")).Do(func(template string, args ...interface{}) {
		wg.Done()
	}).Times(1)

	gc.Add("foo", mockTask)
	gc.Serve()
	gc.Stop()
}
