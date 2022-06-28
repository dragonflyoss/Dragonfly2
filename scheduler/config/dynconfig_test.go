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
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/pkg/rpc/manager"
	"d7y.io/dragonfly/v2/pkg/rpc/manager/client/mocks"
)

func TestDynconfig_GetManagerSourceType(t *testing.T) {
	mockCacheDir := t.TempDir()
	mockConfig := &Config{
		DynConfig: &DynConfig{},
		Server: &ServerConfig{
			Host: "localhost",
		},
		Manager: &ManagerConfig{
			SchedulerClusterID: 1,
		},
	}

	mockCachePath := filepath.Join(mockCacheDir, cacheFileName)
	tests := []struct {
		name            string
		refreshInterval time.Duration
		sleep           func()
		cleanFileCache  func(t *testing.T)
		mock            func(m *mocks.MockClientMockRecorder)
		expect          func(t *testing.T, data *DynconfigData, err error)
	}{
		{
			name:            "get dynconfig success",
			refreshInterval: 10 * time.Second,
			cleanFileCache: func(t *testing.T) {
				if err := os.Remove(mockCachePath); err != nil {
					t.Fatal(err)
				}
			},
			sleep: func() {},
			mock: func(m *mocks.MockClientMockRecorder) {
				m.GetScheduler(gomock.Any()).Return(&manager.Scheduler{
					SeedPeers: []*manager.SeedPeer{
						{
							HostName:     "bar",
							Ip:           "127.0.0.1",
							Port:         8001,
							DownloadPort: 8003,
						},
					},
				}, nil).Times(1)
			},
			expect: func(t *testing.T, data *DynconfigData, err error) {
				assert := assert.New(t)
				assert.Equal(data.SeedPeers[0].Hostname, "bar")
				assert.Equal(data.SeedPeers[0].IP, "127.0.0.1")
				assert.Equal(data.SeedPeers[0].Port, int32(8001))
				assert.Equal(data.SeedPeers[0].DownloadPort, int32(8003))
			},
		},
		{
			name:            "refresh dynconfig",
			refreshInterval: 10 * time.Millisecond,
			cleanFileCache: func(t *testing.T) {
				if err := os.Remove(mockCachePath); err != nil {
					t.Fatal(err)
				}
			},
			sleep: func() {
				time.Sleep(100 * time.Millisecond)
			},
			mock: func(m *mocks.MockClientMockRecorder) {
				gomock.InOrder(
					m.GetScheduler(gomock.Any()).Return(&manager.Scheduler{
						SeedPeers: []*manager.SeedPeer{
							{
								HostName:     "bar",
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
				assert.Equal(data.SeedPeers[0].Hostname, "bar")
				assert.Equal(data.SeedPeers[0].IP, "127.0.0.1")
				assert.Equal(data.SeedPeers[0].Port, int32(8001))
				assert.Equal(data.SeedPeers[0].DownloadPort, int32(8003))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			mockManagerClient := mocks.NewMockClient(ctl)
			tc.mock(mockManagerClient.EXPECT())

			mockConfig.DynConfig.RefreshInterval = tc.refreshInterval
			d, err := NewDynconfig(mockManagerClient, mockCacheDir, mockConfig)
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
