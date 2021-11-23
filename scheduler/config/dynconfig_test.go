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

package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	dc "d7y.io/dragonfly/v2/internal/dynconfig"
	"d7y.io/dragonfly/v2/pkg/rpc/manager"
	"d7y.io/dragonfly/v2/scheduler/config/mocks"
)

func TestDynconfigGet_ManagerSourceType(t *testing.T) {
	tests := []struct {
		name           string
		expire         time.Duration
		sleep          func()
		cleanFileCache func(t *testing.T)
		mock           func(m *mocks.MockClientMockRecorder)
		expect         func(t *testing.T, data *DynconfigData, err error)
	}{
		{
			name:   "get dynconfig success",
			expire: 10 * time.Second,
			cleanFileCache: func(t *testing.T) {
				if err := os.Remove(cachePath); err != nil {
					t.Fatal(err)
				}
			},
			sleep: func() {},
			mock: func(m *mocks.MockClientMockRecorder) {
				m.GetScheduler(gomock.Any()).Return(&manager.Scheduler{
					Cdns: []*manager.CDN{
						{
							HostName:     "foo",
							Ip:           "127.0.0.1",
							Port:         8001,
							DownloadPort: 8003,
						},
					},
				}, nil).Times(1)
			},
			expect: func(t *testing.T, data *DynconfigData, err error) {
				assert := assert.New(t)
				assert.Equal(data.CDNs[0].HostName, "foo")
				assert.Equal(data.CDNs[0].IP, "127.0.0.1")
				assert.Equal(data.CDNs[0].Port, int32(8001))
				assert.Equal(data.CDNs[0].DownloadPort, int32(8003))
			},
		},
		{
			name:   "client failed to return for the second time",
			expire: 10 * time.Millisecond,
			cleanFileCache: func(t *testing.T) {
				if err := os.Remove(cachePath); err != nil {
					t.Fatal(err)
				}
			},
			sleep: func() {
				time.Sleep(100 * time.Millisecond)
			},
			mock: func(m *mocks.MockClientMockRecorder) {
				gomock.InOrder(
					m.GetScheduler(gomock.Any()).Return(&manager.Scheduler{
						Cdns: []*manager.CDN{
							{
								HostName:     "foo",
								Ip:           "127.0.0.1",
								Port:         8001,
								DownloadPort: 8003,
							},
						},
					}, nil).Times(1),
					m.GetScheduler(gomock.Any()).Return(nil, errors.New("foo")).Times(1),
				)
			},
			expect: func(t *testing.T, data *DynconfigData, err error) {
				assert := assert.New(t)
				assert.Equal(data.CDNs[0].HostName, "foo")
				assert.Equal(data.CDNs[0].IP, "127.0.0.1")
				assert.Equal(data.CDNs[0].Port, int32(8001))
				assert.Equal(data.CDNs[0].DownloadPort, int32(8003))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			mockManagerClient := mocks.NewMockClient(ctl)
			tc.mock(mockManagerClient.EXPECT())

			d, err := NewDynconfig(dc.ManagerSourceType, "", []dc.Option{
				dc.WithManagerClient(NewManagerClient(mockManagerClient, &Config{
					Manager: &ManagerConfig{SchedulerClusterID: uint(1)},
					Server:  &ServerConfig{Host: "foo"},
				})),
				dc.WithCachePath(cachePath),
				dc.WithExpireTime(tc.expire),
			}...)
			if err != nil {
				t.Fatal(err)
			}

			tc.sleep()
			data, err := d.Get()
			tc.expect(t, data, err)
			tc.cleanFileCache(t)
		})
	}
}

func TestDynconfigGet_LocalSourceType(t *testing.T) {
	tests := []struct {
		name       string
		configPath string
		expect     func(t *testing.T, data *DynconfigData, err error)
	}{
		{
			name:       "get CDN from local config",
			configPath: filepath.Join("./testdata", "dynconfig", "scheduler.yaml"),
			expect: func(t *testing.T, data *DynconfigData, err error) {
				assert := assert.New(t)
				assert.Equal(
					&DynconfigData{
						CDNs: []*CDN{
							{
								HostName:     "foo",
								IP:           "127.0.0.1",
								Port:         8001,
								DownloadPort: 8003,
							},
							{
								HostName:     "bar",
								IP:           "127.0.0.1",
								Port:         8001,
								DownloadPort: 8003,
							},
						},
					}, data)
			},
		},
		{
			name:       "directory does not exist",
			configPath: filepath.Join("./testdata", "foo"),
			expect: func(t *testing.T, data *DynconfigData, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "open testdata/foo: no such file or directory")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d, err := NewDynconfig(dc.LocalSourceType, "", dc.WithLocalConfigPath(tc.configPath))
			if err != nil {
				t.Fatal(err)
			}

			data, err := d.Get()
			tc.expect(t, data, err)
		})
	}
}

func TestDynconfigGetCDNFromDirPath(t *testing.T) {
	tests := []struct {
		name       string
		cdnDirPath string
		expect     func(t *testing.T, data *DynconfigData, err error)
	}{
		{
			name:       "get CDN from directory",
			cdnDirPath: filepath.Join("./testdata", "dynconfig", "cdn"),
			expect: func(t *testing.T, data *DynconfigData, err error) {
				assert := assert.New(t)
				assert.Equal(data.CDNs[0].HostName, "foo")
				assert.Equal(data.CDNs[1].HostName, "bar")
				assert.Equal(data.CDNs[0].Port, int32(8001))
				assert.Equal(data.CDNs[1].Port, int32(8001))
				assert.Equal(data.CDNs[0].DownloadPort, int32(8003))
				assert.Equal(data.CDNs[1].DownloadPort, int32(8003))
			},
		},
		{
			name:       "directory does not exist",
			cdnDirPath: filepath.Join("./testdata", "foo"),
			expect: func(t *testing.T, data *DynconfigData, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "open testdata/foo: no such file or directory")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {

			d, err := NewDynconfig(dc.LocalSourceType, tc.cdnDirPath, dc.WithLocalConfigPath("./testdata/scheduler.yaml"))
			if err != nil {
				t.Fatal(err)
			}

			data, err := d.Get()
			tc.expect(t, data, err)
		})
	}
}
