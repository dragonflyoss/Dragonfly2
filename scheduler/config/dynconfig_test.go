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

	managerv1 "d7y.io/api/pkg/apis/manager/v1"

	"d7y.io/dragonfly/v2/manager/model"
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
				m.GetScheduler(gomock.Any()).Return(&managerv1.Scheduler{
					Id:          1,
					HostName:    "foo",
					Idc:         "idc",
					Location:    "location",
					NetTopology: "net_topology",
					Ip:          "127.0.0.1",
					Port:        8002,
					State:       "active",
					SeedPeers: []*managerv1.SeedPeer{
						{
							Id:           1,
							HostName:     "bar",
							Type:         model.SeedPeerTypeSuperSeed,
							Idc:          "idc",
							NetTopology:  "net_topology",
							Location:     "location",
							Ip:           "127.0.0.1",
							Port:         8001,
							DownloadPort: 8003,
							SeedPeerCluster: &managerv1.SeedPeerCluster{
								Id:     1,
								Name:   "baz",
								Config: []byte{1},
							},
						},
					},
					SchedulerCluster: &managerv1.SchedulerCluster{
						Id:           1,
						Name:         "bas",
						Config:       []byte{1},
						ClientConfig: []byte{1},
					},
				}, nil).Times(1)
			},
			expect: func(t *testing.T, data *DynconfigData, err error) {
				assert := assert.New(t)
				assert.EqualValues(data, &DynconfigData{
					ID:          1,
					Hostname:    "foo",
					Idc:         "idc",
					Location:    "location",
					NetTopology: "net_topology",
					IP:          "127.0.0.1",
					Port:        8002,
					State:       "active",
					SeedPeers: []*SeedPeer{
						{
							ID:           1,
							Hostname:     "bar",
							Type:         model.SeedPeerTypeSuperSeed,
							IDC:          "idc",
							NetTopology:  "net_topology",
							Location:     "location",
							IP:           "127.0.0.1",
							Port:         8001,
							DownloadPort: 8003,
							SeedPeerCluster: &SeedPeerCluster{
								ID:     1,
								Name:   "baz",
								Config: []byte{1},
							},
						},
					},
					SchedulerCluster: &SchedulerCluster{
						ID:           1,
						Name:         "bas",
						Config:       []byte{1},
						ClientConfig: []byte{1},
					},
				})
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
					m.GetScheduler(gomock.Any()).Return(&managerv1.Scheduler{
						Id:          1,
						HostName:    "foo",
						Idc:         "idc",
						Location:    "location",
						NetTopology: "net_topology",
						Ip:          "127.0.0.1",
						Port:        8002,
						State:       "active",
						SeedPeers: []*managerv1.SeedPeer{
							{
								Id:           1,
								HostName:     "bar",
								Type:         model.SeedPeerTypeSuperSeed,
								Idc:          "idc",
								NetTopology:  "net_topology",
								Location:     "location",
								Ip:           "127.0.0.1",
								Port:         8001,
								DownloadPort: 8003,
								SeedPeerCluster: &managerv1.SeedPeerCluster{
									Id:     1,
									Name:   "baz",
									Config: []byte{1},
								},
							},
						},
						SchedulerCluster: &managerv1.SchedulerCluster{
							Id:           1,
							Name:         "bas",
							Config:       []byte{1},
							ClientConfig: []byte{1},
						},
					}, nil).Times(1),
					m.GetScheduler(gomock.Any()).Return(nil, errors.New("foo")).Times(1),
				)
			},
			expect: func(t *testing.T, data *DynconfigData, err error) {
				assert := assert.New(t)
				assert.EqualValues(data, &DynconfigData{
					ID:          1,
					Hostname:    "foo",
					Idc:         "idc",
					Location:    "location",
					NetTopology: "net_topology",
					IP:          "127.0.0.1",
					Port:        8002,
					State:       "active",
					SeedPeers: []*SeedPeer{
						{
							ID:           1,
							Hostname:     "bar",
							Type:         model.SeedPeerTypeSuperSeed,
							IDC:          "idc",
							NetTopology:  "net_topology",
							Location:     "location",
							IP:           "127.0.0.1",
							Port:         8001,
							DownloadPort: 8003,
							SeedPeerCluster: &SeedPeerCluster{
								ID:     1,
								Name:   "baz",
								Config: []byte{1},
							},
						},
					},
					SchedulerCluster: &SchedulerCluster{
						ID:           1,
						Name:         "bas",
						Config:       []byte{1},
						ClientConfig: []byte{1},
					},
				})
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
