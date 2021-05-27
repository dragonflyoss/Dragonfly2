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
	"testing"
	"time"

	"d7y.io/dragonfly/v2/pkg/gc/mocks"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestGCNew(t *testing.T) {
	tests := []struct {
		name     string
		timeout  time.Duration
		interval time.Duration
		mock     func(ml *mocks.MockLoggerMockRecorder, mt *mocks.MockTaskMockRecorder)
		expect   func(t *testing.T, err error)
	}{
		{
			name:     "new GC instance succeeded",
			timeout:  1 * time.Second,
			interval: 2 * time.Second,
			mock:     func(ml *mocks.MockLoggerMockRecorder, mt *mocks.MockTaskMockRecorder) {},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
		{
			name:     "new GC without interval",
			timeout:  1 * time.Second,
			interval: 0,
			mock:     func(ml *mocks.MockLoggerMockRecorder, mt *mocks.MockTaskMockRecorder) {},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "interval value is greater than 0")
			},
		},
		{
			name:     "new GC without timeout",
			timeout:  0,
			interval: 2 * time.Second,
			mock:     func(ml *mocks.MockLoggerMockRecorder, mt *mocks.MockTaskMockRecorder) {},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
		{
			name:     "timeout is greater than interval",
			timeout:  2 * time.Second,
			interval: 1 * time.Second,
			mock:     func(ml *mocks.MockLoggerMockRecorder, mt *mocks.MockTaskMockRecorder) {},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "timeout value needs to be less than the interval value")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			mockLogger := mocks.NewMockLogger(ctl)
			mockTask := mocks.NewMockTask(ctl)
			tc.mock(mockLogger.EXPECT(), mockTask.EXPECT())

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
		timeout  time.Duration
		interval time.Duration
		mock     func(ml *mocks.MockLoggerMockRecorder, mt *mocks.MockTaskMockRecorder)
		expect   func(t *testing.T, err error)
	}{
		{
			name:     "new GC instance succeeded",
			taskName: "foo",
			timeout:  1 * time.Second,
			interval: 2 * time.Second,
			mock:     func(ml *mocks.MockLoggerMockRecorder, mt *mocks.MockTaskMockRecorder) {},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
		{
			name:     "new GC without interval",
			timeout:  1 * time.Second,
			interval: 0,
			mock:     func(ml *mocks.MockLoggerMockRecorder, mt *mocks.MockTaskMockRecorder) {},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "interval value is greater than 0")
			},
		},
		{
			name:     "new GC without timeout",
			timeout:  0,
			interval: 2 * time.Second,
			mock:     func(ml *mocks.MockLoggerMockRecorder, mt *mocks.MockTaskMockRecorder) {},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
		{
			name:     "timeout is greater than interval",
			timeout:  2 * time.Second,
			interval: 1 * time.Second,
			mock:     func(ml *mocks.MockLoggerMockRecorder, mt *mocks.MockTaskMockRecorder) {},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "timeout value needs to be less than the interval value")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			mockLogger := mocks.NewMockLogger(ctl)
			mockTask := mocks.NewMockTask(ctl)
			tc.mock(mockLogger.EXPECT(), mockTask.EXPECT())

			_, err := New([]Option{
				WithInterval(tc.interval),
				WithTimeout(tc.timeout),
				WithLogger(mockLogger),
			}...)

			tc.expect(t, err)
		})
	}
}

func TestGCRunAll(t *testing.T) {
}

func TestGCServe(t *testing.T) {
}
