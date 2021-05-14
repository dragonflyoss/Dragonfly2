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

	mock_manager_client "d7y.io/dragonfly/v2/pkg/dynconfig/mocks"
	"github.com/golang/mock/gomock"
	"github.com/mitchellh/mapstructure"
	"github.com/stretchr/testify/assert"
)

type TestDynconfig struct {
	Scheduler SchedulerOption
}

type SchedulerOption struct {
	Name string
}

func TestDynconfigGet_ManagerSourceType(t *testing.T) {
	schedulerName := "scheduler"
	tests := []struct {
		name           string
		expire         time.Duration
		dynconfig      TestDynconfig
		sleep          func()
		cleanFileCache func(t *testing.T)
		mock           func(m *mock_manager_client.MockmanagerClientMockRecorder)
		expect         func(t *testing.T, data interface{})
	}{
		{
			name:   "get dynconfig success without file cache",
			expire: 20 * time.Millisecond,
			dynconfig: TestDynconfig{
				Scheduler: SchedulerOption{
					Name: schedulerName,
				},
			},
			sleep:          func() {},
			cleanFileCache: func(t *testing.T) {},
			mock: func(m *mock_manager_client.MockmanagerClientMockRecorder) {
				var d map[string]interface{}
				mapstructure.Decode(TestDynconfig{
					Scheduler: SchedulerOption{
						Name: schedulerName,
					},
				}, &d)
				m.Get().Return(d, nil).Times(1)
			},
			expect: func(t *testing.T, data interface{}) {
				assert := assert.New(t)
				var d TestDynconfig
				mapstructure.Decode(data, &d)
				assert.EqualValues(d, TestDynconfig{
					Scheduler: SchedulerOption{
						Name: schedulerName,
					},
				})
			},
		},
		{
			name:   "get expire dynconfig with file cache",
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
				path, err := DefaultCacheFile()
				if err != nil {
					t.Fatal(err)
				}

				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			},
			mock: func(m *mock_manager_client.MockmanagerClientMockRecorder) {
				var d map[string]interface{}
				mapstructure.Decode(TestDynconfig{
					Scheduler: SchedulerOption{
						Name: schedulerName,
					},
				}, &d)
				m.Get().Return(d, nil).Times(1)
			},
			expect: func(t *testing.T, data interface{}) {
				assert := assert.New(t)
				var d TestDynconfig
				mapstructure.Decode(data, &d)
				assert.EqualValues(d, TestDynconfig{
					Scheduler: SchedulerOption{
						Name: schedulerName,
					},
				})
			},
		},
		{
			name:   "get manager config failed",
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
				path, err := DefaultCacheFile()
				if err != nil {
					t.Fatal(err)
				}

				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			},
			mock: func(m *mock_manager_client.MockmanagerClientMockRecorder) {
				var d map[string]interface{}
				mapstructure.Decode(TestDynconfig{
					Scheduler: SchedulerOption{
						Name: schedulerName,
					},
				}, &d)
				m.Get().Return(d, nil).Times(1)
				m.Get().Return(nil, errors.New("manager serivce error")).Times(1)
			},
			expect: func(t *testing.T, data interface{}) {
				assert := assert.New(t)
				var d TestDynconfig
				mapstructure.Decode(data, &d)
				assert.EqualValues(d, TestDynconfig{
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
			mockManagerClient := mock_manager_client.NewMockmanagerClient(ctl)
			tc.mock(mockManagerClient.EXPECT())

			d, err := New(ManagerSourceType, tc.expire, []Option{WithManagerClient(mockManagerClient)}...)
			if err != nil {
				t.Fatal(err)
			}

			tc.sleep()
			data, err := d.Get()
			if err != nil {
				t.Fatal(err)
			}

			tc.expect(t, data)
			tc.cleanFileCache(t)
		})
	}
}

func TestDynconfigUnmarshal_ManagerSourceType(t *testing.T) {
	schedulerName := "scheduler"
	tests := []struct {
		name           string
		expire         time.Duration
		dynconfig      TestDynconfig
		sleep          func()
		cleanFileCache func(t *testing.T)
		mock           func(m *mock_manager_client.MockmanagerClientMockRecorder)
		expect         func(t *testing.T, data interface{})
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
			mock: func(m *mock_manager_client.MockmanagerClientMockRecorder) {
				var d map[string]interface{}
				mapstructure.Decode(TestDynconfig{
					Scheduler: SchedulerOption{
						Name: schedulerName,
					},
				}, &d)
				m.Get().Return(d, nil).Times(1)
			},
			expect: func(t *testing.T, data interface{}) {
				assert := assert.New(t)
				var d TestDynconfig
				mapstructure.Decode(data, &d)
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
				path, err := DefaultCacheFile()
				if err != nil {
					t.Fatal(err)
				}

				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			},
			mock: func(m *mock_manager_client.MockmanagerClientMockRecorder) {
				var d map[string]interface{}
				mapstructure.Decode(TestDynconfig{
					Scheduler: SchedulerOption{
						Name: schedulerName,
					},
				}, &d)
				m.Get().Return(d, nil).Times(1)
			},
			expect: func(t *testing.T, data interface{}) {
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
				path, err := DefaultCacheFile()
				if err != nil {
					t.Fatal(err)
				}

				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			},
			mock: func(m *mock_manager_client.MockmanagerClientMockRecorder) {
				var d map[string]interface{}
				mapstructure.Decode(TestDynconfig{
					Scheduler: SchedulerOption{
						Name: schedulerName,
					},
				}, &d)
				m.Get().Return(d, nil).Times(1)
				m.Get().Return(nil, errors.New("manager serivce error")).Times(1)
			},
			expect: func(t *testing.T, data interface{}) {
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
			mockManagerClient := mock_manager_client.NewMockmanagerClient(ctl)
			tc.mock(mockManagerClient.EXPECT())

			d, err := New(ManagerSourceType, tc.expire, []Option{WithManagerClient(mockManagerClient)}...)
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

func TestDynconfigGet_LocalSourceType(t *testing.T) {
	schedulerName := "scheduler"
	configPath := filepath.Join("./testdata", "dynconfig.yaml")

	tests := []struct {
		name       string
		expire     time.Duration
		dynconfig  TestDynconfig
		configPath string
		sleep      func()
		expect     func(t *testing.T, data interface{})
	}{
		{
			name:   "get dynconfig success with local file",
			expire: 20 * time.Millisecond,
			dynconfig: TestDynconfig{
				Scheduler: SchedulerOption{
					Name: schedulerName,
				},
			},
			configPath: configPath,
			sleep:      func() {},
			expect: func(t *testing.T, data interface{}) {
				assert := assert.New(t)
				var d TestDynconfig
				mapstructure.Decode(data, &d)
				assert.EqualValues(d, TestDynconfig{
					Scheduler: SchedulerOption{
						Name: schedulerName,
					},
				})
			},
		},
		{
			name:   "get expire dynconfig with local file",
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
			expect: func(t *testing.T, data interface{}) {
				assert := assert.New(t)
				var d TestDynconfig
				mapstructure.Decode(data, &d)
				assert.EqualValues(d, TestDynconfig{
					Scheduler: SchedulerOption{
						Name: schedulerName,
					},
				})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d, err := New(LocalSourceType, tc.expire, []Option{WithLocalConfigPath(configPath)}...)
			if err != nil {
				t.Fatal(err)
			}

			tc.sleep()
			data, err := d.Get()
			if err != nil {
				t.Fatal(err)
			}

			tc.expect(t, data)
		})
	}
}

func TestDynconfigUnmarshal_LocalSourceType(t *testing.T) {
	schedulerName := "scheduler"
	configPath := filepath.Join("./testdata", "dynconfig.yaml")

	tests := []struct {
		name       string
		expire     time.Duration
		dynconfig  TestDynconfig
		configPath string
		sleep      func()
		expect     func(t *testing.T, data interface{})
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
			expect: func(t *testing.T, data interface{}) {
				assert := assert.New(t)
				var d TestDynconfig
				mapstructure.Decode(data, &d)
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
			expect: func(t *testing.T, data interface{}) {
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
			d, err := New(LocalSourceType, tc.expire, []Option{WithLocalConfigPath(configPath)}...)
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
