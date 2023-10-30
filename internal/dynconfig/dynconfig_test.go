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

package dynconfig

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mitchellh/mapstructure"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"d7y.io/dragonfly/v2/internal/dynconfig/mocks"
)

type TestDynconfig struct {
	Scheduler SchedulerOption
}

type SchedulerOption struct {
	Name string
}

func TestDynconfig_Get(t *testing.T) {
	schedulerName := "scheduler"
	cachePath, err := mockCachePath()
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name           string
		expire         time.Duration
		sleep          func()
		cleanFileCache func(t *testing.T)
		mock           func(m *mocks.MockManagerClientMockRecorder)
		run            func(t *testing.T, d Dynconfig[TestDynconfig])
	}{
		{
			name:           "get config success without file cache",
			expire:         1 * time.Millisecond,
			sleep:          func() {},
			cleanFileCache: func(t *testing.T) {},
			mock: func(m *mocks.MockManagerClientMockRecorder) {
				var d map[string]any
				if err := mapstructure.Decode(TestDynconfig{
					Scheduler: SchedulerOption{
						Name: schedulerName,
					},
				}, &d); err != nil {
					t.Error(err)
				}

				m.Get().Return(d, nil).AnyTimes()
			},
			run: func(t *testing.T, d Dynconfig[TestDynconfig]) {
				assert := assert.New(t)
				data, err := d.Get()
				assert.NoError(err)
				assert.EqualValues(data, &TestDynconfig{
					Scheduler: SchedulerOption{
						Name: schedulerName,
					},
				})
			},
		},
		{
			name:   "get expire config with file cache",
			expire: 1 * time.Millisecond,
			sleep: func() {
				time.Sleep(30 * time.Millisecond)
			},
			cleanFileCache: func(t *testing.T) {
				if err := os.Remove(cachePath); err != nil {
					t.Fatal(err)
				}
			},
			mock: func(m *mocks.MockManagerClientMockRecorder) {
				var d map[string]any
				if err := mapstructure.Decode(TestDynconfig{
					Scheduler: SchedulerOption{
						Name: "foo",
					},
				}, &d); err != nil {
					t.Error(err)
				}

				m.Get().Return(d, nil).Times(2)
			},
			run: func(t *testing.T, d Dynconfig[TestDynconfig]) {
				assert := assert.New(t)
				data, err := d.Get()
				assert.NoError(err)
				assert.EqualValues(data, &TestDynconfig{
					Scheduler: SchedulerOption{
						Name: "foo",
					},
				})
			},
		},
		{
			name:   "get config failed",
			expire: 1 * time.Millisecond,
			sleep: func() {
				time.Sleep(30 * time.Millisecond)
			},
			cleanFileCache: func(t *testing.T) {
				if err := os.Remove(cachePath); err != nil {
					t.Fatal(err)
				}
			},
			mock: func(m *mocks.MockManagerClientMockRecorder) {
				var d map[string]any
				if err := mapstructure.Decode(TestDynconfig{
					Scheduler: SchedulerOption{
						Name: schedulerName,
					},
				}, &d); err != nil {
					t.Error(err)
				}

				gomock.InOrder(
					m.Get().Return(d, nil).Times(1),
					m.Get().Return(nil, errors.New("manager serivce error")).Times(1),
				)
			},
			run: func(t *testing.T, d Dynconfig[TestDynconfig]) {
				assert := assert.New(t)
				data, err := d.Get()
				assert.NoError(err)
				assert.EqualValues(data, &TestDynconfig{
					Scheduler: SchedulerOption{
						Name: schedulerName,
					},
				})
			},
		},
		{
			name:   "get config with file cache",
			expire: 10 * time.Second,
			sleep:  func() {},
			cleanFileCache: func(t *testing.T) {
				if err := os.Remove(cachePath); err != nil {
					t.Fatal(err)
				}
			},
			mock: func(m *mocks.MockManagerClientMockRecorder) {
				var d map[string]any
				if err := mapstructure.Decode(TestDynconfig{
					Scheduler: SchedulerOption{
						Name: schedulerName,
					},
				}, &d); err != nil {
					t.Error(err)
				}

				m.Get().Return(d, nil).Times(1)
			},
			run: func(t *testing.T, d Dynconfig[TestDynconfig]) {
				assert := assert.New(t)
				data, err := d.Get()
				assert.NoError(err)
				assert.EqualValues(data, &TestDynconfig{
					Scheduler: SchedulerOption{
						Name: schedulerName,
					},
				})

				data, err = d.Get()
				assert.NoError(err)
				assert.EqualValues(data, &TestDynconfig{
					Scheduler: SchedulerOption{
						Name: schedulerName,
					},
				})
			},
		},
		{
			name:   "config is changed",
			expire: 20 * time.Millisecond,
			sleep: func() {
				time.Sleep(30 * time.Millisecond)
			},
			cleanFileCache: func(t *testing.T) {
				if err := os.Remove(cachePath); err != nil {
					t.Fatal(err)
				}
			},
			mock: func(m *mocks.MockManagerClientMockRecorder) {
				var df map[string]any
				if err := mapstructure.Decode(TestDynconfig{
					Scheduler: SchedulerOption{
						Name: schedulerName,
					},
				}, &df); err != nil {
					t.Error(err)
				}

				var ds map[string]any
				if err := mapstructure.Decode(TestDynconfig{
					Scheduler: SchedulerOption{
						Name: "foo",
					},
				}, &ds); err != nil {
					t.Error(err)
				}

				gomock.InOrder(
					m.Get().Return(df, nil).Times(2),
					m.Get().Return(ds, nil).Times(1),
				)
			},
			run: func(t *testing.T, d Dynconfig[TestDynconfig]) {
				assert := assert.New(t)
				data, err := d.Get()
				assert.NoError(err)
				assert.EqualValues(data, &TestDynconfig{
					Scheduler: SchedulerOption{
						Name: schedulerName,
					},
				})

				data, err = d.Get()
				assert.NoError(err)
				assert.EqualValues(data, &TestDynconfig{
					Scheduler: SchedulerOption{
						Name: schedulerName,
					},
				})

				time.Sleep(30 * time.Millisecond)
				data, err = d.Get()
				assert.NoError(err)
				assert.EqualValues(data, &TestDynconfig{
					Scheduler: SchedulerOption{
						Name: "foo",
					},
				})
			},
		},
		{
			name:   "config is not changed",
			expire: 1 * time.Millisecond,
			sleep: func() {
				time.Sleep(30 * time.Millisecond)
			},
			cleanFileCache: func(t *testing.T) {
				if err := os.Remove(cachePath); err != nil {
					t.Fatal(err)
				}
			},
			mock: func(m *mocks.MockManagerClientMockRecorder) {
				var df map[string]any
				if err := mapstructure.Decode(TestDynconfig{
					Scheduler: SchedulerOption{
						Name: schedulerName,
					},
				}, &df); err != nil {
					t.Error(err)
				}

				m.Get().Return(df, nil).Times(3)
			},
			run: func(t *testing.T, d Dynconfig[TestDynconfig]) {
				assert := assert.New(t)
				data, err := d.Get()
				assert.NoError(err)
				assert.EqualValues(data, &TestDynconfig{
					Scheduler: SchedulerOption{
						Name: schedulerName,
					},
				})

				time.Sleep(30 * time.Millisecond)
				data, err = d.Get()
				assert.NoError(err)
				assert.EqualValues(data, &TestDynconfig{
					Scheduler: SchedulerOption{
						Name: schedulerName,
					},
				})
			},
		},
		{
			name:   "config is not changed and cache is not expired",
			expire: 10 * time.Second,
			sleep:  func() {},
			cleanFileCache: func(t *testing.T) {
				if err := os.Remove(cachePath); err != nil {
					t.Fatal(err)
				}
			},
			mock: func(m *mocks.MockManagerClientMockRecorder) {
				var df map[string]any
				if err := mapstructure.Decode(TestDynconfig{
					Scheduler: SchedulerOption{
						Name: schedulerName,
					},
				}, &df); err != nil {
					t.Error(err)
				}

				m.Get().Return(df, nil).Times(1)
			},
			run: func(t *testing.T, d Dynconfig[TestDynconfig]) {
				assert := assert.New(t)
				data, err := d.Get()
				assert.NoError(err)
				assert.EqualValues(data, &TestDynconfig{
					Scheduler: SchedulerOption{
						Name: schedulerName,
					},
				})

				data, err = d.Get()
				assert.NoError(err)
				assert.EqualValues(data, &TestDynconfig{
					Scheduler: SchedulerOption{
						Name: schedulerName,
					},
				})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			mockManagerClient := mocks.NewMockManagerClient(ctl)
			tc.mock(mockManagerClient.EXPECT())

			d, err := New[TestDynconfig](mockManagerClient, cachePath, tc.expire)
			if err != nil {
				t.Fatal(err)
			}

			tc.sleep()
			tc.run(t, d)
			tc.cleanFileCache(t)
		})
	}
}

func mockCachePath() (string, error) {
	userDir, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}

	return filepath.Join(userDir, ".dynconfig"), nil
}
