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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"

	"d7y.io/dragonfly/v2/internal/dynconfig"
	"d7y.io/dragonfly/v2/pkg/rpc/manager"
	"d7y.io/dragonfly/v2/pkg/rpc/manager/client/mocks"
)

func TestGetFromManagerSourceType(t *testing.T) {
	cacheDir := t.TempDir()

	tests := []struct {
		name              string
		cfg               Config
		mock              func(m *mocks.MockClientMockRecorder)
		beforeSleepExpect func(t *testing.T, cdn *manager.CDN, err error)
		sleepFunc         func() error
		afterSleepExpect  func(t *testing.T, cdn *manager.CDN, err error)
	}{
		{
			name: "get config success",
			cfg: Config{
				RefreshInterval: 10 * time.Millisecond,
				CachePath:       filepath.Join(cacheDir, "test1"),
				SourceType:      dynconfig.ManagerSourceType,
			},
			mock: func(m *mocks.MockClientMockRecorder) {
				m.GetCDN(gomock.Any()).Return(&manager.CDN{
					Schedulers: []*manager.Scheduler{
						{
							HostName: "host1",
							Ip:       "127.0.0.1",
							Port:     9000,
						}, {
							HostName: "host2",
							Ip:       "127.0.0.2",
							Port:     9000,
						},
					},
				}, nil).Times(1)
			},
			beforeSleepExpect: func(t *testing.T, cdn *manager.CDN, err error) {
				assert := assert.New(t)
				assert.Nil(err)
				assert.Equal(2, len(cdn.Schedulers))
				assert.Equal(cdn.Schedulers[0].HostName, "host1")
				assert.Equal(cdn.Schedulers[0].Ip, "127.0.0.1")
				assert.EqualValues(cdn.Schedulers[0].Port, 9000)
				assert.Equal(cdn.Schedulers[1].HostName, "host2")
				assert.Equal(cdn.Schedulers[1].Ip, "127.0.0.2")
				assert.EqualValues(cdn.Schedulers[1].Port, 9000)
			},
			sleepFunc: func() error { return nil },
			afterSleepExpect: func(t *testing.T, cdn *manager.CDN, err error) {
				assert := assert.New(t)
				assert.Nil(err)
				assert.Equal(2, len(cdn.Schedulers))
				assert.Equal(cdn.Schedulers[0].HostName, "host1")
				assert.Equal(cdn.Schedulers[0].Ip, "127.0.0.1")
				assert.EqualValues(cdn.Schedulers[0].Port, 9000)
				assert.Equal(cdn.Schedulers[1].HostName, "host2")
				assert.Equal(cdn.Schedulers[1].Ip, "127.0.0.2")
				assert.EqualValues(cdn.Schedulers[1].Port, 9000)
			},
		},
		{
			name: "get config success and refresh",
			cfg: Config{
				RefreshInterval: 10 * time.Millisecond,
				CachePath:       filepath.Join(cacheDir, "test2"),
				SourceType:      dynconfig.ManagerSourceType,
			},
			mock: func(m *mocks.MockClientMockRecorder) {
				gomock.InOrder(
					m.GetCDN(gomock.Any()).Return(&manager.CDN{
						Schedulers: []*manager.Scheduler{
							{
								HostName: "host1",
								Ip:       "127.0.0.1",
								Port:     9000,
							},
						},
					}, nil).Times(1),
					m.GetCDN(gomock.Any()).Return(&manager.CDN{
						Schedulers: []*manager.Scheduler{
							{
								HostName: "host2",
								Ip:       "127.0.0.2",
								Port:     9000,
							},
						},
					}, nil).Times(1),
				)
			},
			beforeSleepExpect: func(t *testing.T, cdn *manager.CDN, err error) {
				assert := assert.New(t)
				assert.Equal(1, len(cdn.Schedulers))
				assert.Equal(cdn.Schedulers[0].HostName, "host1")
				assert.Equal(cdn.Schedulers[0].Ip, "127.0.0.1")
				assert.EqualValues(cdn.Schedulers[0].Port, 9000)
			},
			sleepFunc: func() error {
				time.Sleep(15 * time.Millisecond)
				return nil
			},
			afterSleepExpect: func(t *testing.T, cdn *manager.CDN, err error) {
				assert := assert.New(t)
				assert.Equal(1, len(cdn.Schedulers))
				assert.Equal(cdn.Schedulers[0].HostName, "host2")
				assert.Equal(cdn.Schedulers[0].Ip, "127.0.0.2")
				assert.EqualValues(cdn.Schedulers[0].Port, 9000)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			mockManagerClient := mocks.NewMockClient(ctl)
			tc.mock(mockManagerClient.EXPECT())
			var drawFun = func() (interface{}, error) {
				cdn, err := mockManagerClient.GetCDN(&manager.GetCDNRequest{
					HostName:     "cdn",
					SourceType:   manager.SourceType_SCHEDULER_SOURCE,
					CdnClusterId: 10,
				})
				if err != nil {
					return nil, err
				}
				return cdn, nil
			}
			d, err := NewDynconfig(tc.cfg, drawFun)
			if err != nil {
				t.Fatal(err)
			}
			cdn := new(manager.CDN)
			err = d.Get(cdn)
			tc.beforeSleepExpect(t, cdn, err)
			tc.sleepFunc()
			err = d.Get(cdn)
			tc.afterSleepExpect(t, cdn, err)
			d.Stop()
		})
	}
	assert.Nil(t, os.RemoveAll(cacheDir))
}

func TestGetDataFromLocalType(t *testing.T) {
	cacheDir := t.TempDir()
	cachePath1 := filepath.Join(cacheDir, "config1")
	cachePath2 := filepath.Join(cacheDir, "config2")
	tests := []struct {
		name              string
		cfg               Config
		initDataFunc      func(cachePath string) error
		beforeSleepExcept func(t *testing.T, cdn *manager.CDN, err error)
		sleepFunc         func(cachePath string) error
		afterSleepExpect  func(t *testing.T, cdn *manager.CDN, err error)
	}{
		{
			name: "get config successfully from path",
			cfg: Config{
				RefreshInterval: 100 * time.Millisecond,
				CachePath:       cachePath1,
				SourceType:      dynconfig.LocalSourceType,
			},
			initDataFunc: func(cachePath string) error {
				file, err := os.Create(cachePath1)
				if err != nil {
					return err
				}
				bytes, err := yaml.Marshal(&manager.CDN{
					Schedulers: []*manager.Scheduler{
						{
							HostName: "host1",
							Ip:       "127.0.0.1",
							Port:     9000,
						}, {
							HostName: "host2",
							Ip:       "127.0.0.2",
							Port:     9000,
						},
					},
				})
				if err != nil {
					return err
				}
				file.Write(bytes)
				return file.Sync()
			},
			beforeSleepExcept: func(t *testing.T, cdn *manager.CDN, err error) {
				assert := assert.New(t)
				assert.Equal(2, len(cdn.Schedulers))
				assert.Equal(cdn.Schedulers[0].HostName, "host1")
				assert.Equal(cdn.Schedulers[0].Ip, "127.0.0.1")
				assert.EqualValues(cdn.Schedulers[0].Port, 9000)
				assert.Equal(cdn.Schedulers[1].HostName, "host2")
				assert.Equal(cdn.Schedulers[1].Ip, "127.0.0.2")
				assert.EqualValues(cdn.Schedulers[1].Port, 9000)
			},
			sleepFunc: func(cachePath string) error {
				time.Sleep(10 * time.Millisecond)
				return nil
			},
			afterSleepExpect: func(t *testing.T, cdn *manager.CDN, err error) {
				assert := assert.New(t)
				assert.Equal(2, len(cdn.Schedulers))
				assert.Equal(cdn.Schedulers[0].HostName, "host1")
				assert.Equal(cdn.Schedulers[0].Ip, "127.0.0.1")
				assert.EqualValues(cdn.Schedulers[0].Port, 9000)
				assert.Equal(cdn.Schedulers[1].HostName, "host2")
				assert.Equal(cdn.Schedulers[1].Ip, "127.0.0.2")
				assert.EqualValues(cdn.Schedulers[1].Port, 9000)
			},
		},
		{
			name: "get config successfully from path and refresh data",
			cfg: Config{
				RefreshInterval: 10 * time.Millisecond,
				CachePath:       cachePath2,
				SourceType:      dynconfig.LocalSourceType,
			},
			initDataFunc: func(cachePath string) error {
				file, err := os.Create(cachePath)
				if err != nil {
					return err
				}
				bytes, err := yaml.Marshal(&manager.CDN{
					Schedulers: []*manager.Scheduler{
						{
							HostName: "host1",
							Ip:       "127.0.0.1",
							Port:     9000,
						}, {
							HostName: "host2",
							Ip:       "127.0.0.2",
							Port:     9000,
						},
					},
				})
				if err != nil {
					return err
				}
				file.Write(bytes)
				return file.Sync()
			},
			beforeSleepExcept: func(t *testing.T, cdn *manager.CDN, err error) {
				assert := assert.New(t)
				assert.Equal(2, len(cdn.Schedulers))
				assert.Equal(cdn.Schedulers[0].HostName, "host1")
				assert.Equal(cdn.Schedulers[0].Ip, "127.0.0.1")
				assert.EqualValues(cdn.Schedulers[0].Port, 9000)
				assert.Equal(cdn.Schedulers[1].HostName, "host2")
				assert.Equal(cdn.Schedulers[1].Ip, "127.0.0.2")
				assert.EqualValues(cdn.Schedulers[1].Port, 9000)
			},
			sleepFunc: func(cachePath string) error {
				file, err := os.Create(cachePath)
				if err != nil {
					return err
				}
				bytes, err := yaml.Marshal(&manager.CDN{
					Schedulers: []*manager.Scheduler{
						{
							HostName: "host3",
							Ip:       "127.0.0.3",
							Port:     9000,
						}, {
							HostName: "host4",
							Ip:       "127.0.0.4",
							Port:     9000,
						},
					},
				})
				if err != nil {
					return err
				}
				file.Write(bytes)
				return file.Sync()
				time.Sleep(10 * time.Millisecond)
				return nil
			},
			afterSleepExpect: func(t *testing.T, cdn *manager.CDN, err error) {
				assert := assert.New(t)
				assert.Equal(2, len(cdn.Schedulers))
				assert.Equal(cdn.Schedulers[0].HostName, "host3")
				assert.Equal(cdn.Schedulers[0].Ip, "127.0.0.3")
				assert.EqualValues(cdn.Schedulers[0].Port, 9000)
				assert.Equal(cdn.Schedulers[1].HostName, "host4")
				assert.Equal(cdn.Schedulers[1].Ip, "127.0.0.4")
				assert.EqualValues(cdn.Schedulers[1].Port, 9000)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.initDataFunc(tc.cfg.CachePath)
			d, err := NewDynconfig(tc.cfg, func() (interface{}, error) {
				b, err := os.ReadFile(tc.cfg.CachePath)
				if err != nil {
					return nil, err
				}
				cdn := new(manager.CDN)
				if err = yaml.Unmarshal(b, cdn); err != nil {
					return nil, err
				}
				return cdn, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			cdn := new(manager.CDN)
			err = d.Get(cdn)
			tc.beforeSleepExcept(t, cdn, err)
			assert.Nil(t, tc.sleepFunc(tc.cfg.CachePath))
			cdn = new(manager.CDN)
			err = d.Get(cdn)
			tc.afterSleepExpect(t, cdn, err)
			d.Stop()
		})
	}
	assert.Nil(t, os.RemoveAll(cacheDir))
}
