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

	"github.com/golang/mock/gomock"
	"github.com/mitchellh/mapstructure"
	"github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/internal/dynconfig/mocks"
)

type TestDynconfig struct {
	Scheduler SchedulerOption
}

type SchedulerOption struct {
	Name string
}

func TestDynconfigUnmarshal_ManagerSourceType(t *testing.T) {
	schedulerName := "scheduler"
	cachePath, err := mockCachePath()
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name           string
		expire         time.Duration
		dynconfig      TestDynconfig
		sleep          func()
		cleanFileCache func(t *testing.T)
		mock           func(m *mocks.MockManagerClientMockRecorder)
		expect         func(t *testing.T, data any)
	}{
		{
			name:   "unmarshals dynconfig success without file cache",
			expire: 20 * time.Millisecond,
			dynconfig: TestDynconfig{
				Scheduler: SchedulerOption{
					Name: schedulerName,
				},
			},
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
			expect: func(t *testing.T, data any) {
				assert := assert.New(t)
				var d TestDynconfig
				if err := mapstructure.Decode(data, &d); err != nil {
					t.Error(err)
				}

				assert.EqualValues(d, TestDynconfig{
					Scheduler: SchedulerOption{
						Name: schedulerName,
					},
				})
			},
		},
		{
			name:   "unmarshals expire dynconfig with file cache",
			expire: 20 * time.Millisecond,
			dynconfig: TestDynconfig{
				Scheduler: SchedulerOption{
					Name: schedulerName,
				},
			},
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

				m.Get().Return(d, nil).Times(1)
			},
			expect: func(t *testing.T, data any) {
				assert := assert.New(t)
				assert.EqualValues(data, TestDynconfig{
					Scheduler: SchedulerOption{
						Name: schedulerName,
					},
				})
			},
		},
		{
			name:   "unmarshals manager config failed",
			expire: 20 * time.Millisecond,
			dynconfig: TestDynconfig{
				Scheduler: SchedulerOption{
					Name: schedulerName,
				},
			},
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

				m.Get().Return(d, nil).Times(1)
				m.Get().Return(nil, errors.New("manager serivce error")).Times(1)
			},
			expect: func(t *testing.T, data any) {
				assert := assert.New(t)
				assert.EqualValues(data, TestDynconfig{
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

			d, err := New(ManagerSourceType, []Option{
				WithManagerClient(mockManagerClient),
				WithCachePath(cachePath),
				WithExpireTime(tc.expire),
			}...)
			if err != nil {
				t.Fatal(err)
			}

			tc.sleep()
			var data TestDynconfig
			if err := d.Unmarshal(&data); err != nil {
				t.Fatal(err)
			}

			tc.expect(t, data)
			tc.cleanFileCache(t)
		})
	}
}

func TestDynconfigUnmarshal_LocalSourceType(t *testing.T) {
	schedulerName := "foo"
	configPath := filepath.Join("./testdata", "dynconfig.json")

	tests := []struct {
		name       string
		expire     time.Duration
		dynconfig  TestDynconfig
		configPath string
		sleep      func()
		expect     func(t *testing.T, data any)
	}{
		{
			name:   "unmarshals dynconfig success with local file",
			expire: 20 * time.Millisecond,
			dynconfig: TestDynconfig{
				Scheduler: SchedulerOption{
					Name: schedulerName,
				},
			},
			configPath: configPath,
			sleep:      func() {},
			expect: func(t *testing.T, data any) {
				assert := assert.New(t)
				var d TestDynconfig
				if err := mapstructure.Decode(data, &d); err != nil {
					t.Error(err)
				}

				assert.EqualValues(d, TestDynconfig{
					Scheduler: SchedulerOption{
						Name: schedulerName,
					},
				})
			},
		},
		{
			name:   "unmarshals expire dynconfig with local file",
			expire: 20 * time.Millisecond,
			dynconfig: TestDynconfig{
				Scheduler: SchedulerOption{
					Name: schedulerName,
				},
			},
			configPath: configPath,
			sleep: func() {
				time.Sleep(30 * time.Millisecond)
			},
			expect: func(t *testing.T, data any) {
				assert := assert.New(t)
				assert.EqualValues(data, TestDynconfig{
					Scheduler: SchedulerOption{
						Name: schedulerName,
					},
				})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d, err := New(LocalSourceType, []Option{WithLocalConfigPath(configPath)}...)
			if err != nil {
				t.Fatal(err)
			}

			tc.sleep()
			var data TestDynconfig
			if err := d.Unmarshal(&data); err != nil {
				t.Fatal(err)
			}

			tc.expect(t, data)
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
