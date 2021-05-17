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
	"testing"
	"time"

	dc "d7y.io/dragonfly/v2/pkg/dynconfig"
	"d7y.io/dragonfly/v2/pkg/rpc/manager"
	mock_manager_client "d7y.io/dragonfly/v2/scheduler/config/mocks"
	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestDynconfigGet(t *testing.T) {
	tests := []struct {
		name           string
		expire         time.Duration
		sleep          func()
		cleanFileCache func(t *testing.T)
		mock           func(m *mock_manager_client.MockManagerClientMockRecorder)
		expect         func(t *testing.T, data *manager.SchedulerConfig, err error)
	}{
		{
			name:   "get dynconfig success",
			expire: 10 * time.Second,
			cleanFileCache: func(t *testing.T) {
				path, err := dc.DefaultCacheFile()
				if err != nil {
					t.Fatal(err)
				}

				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			},
			sleep: func() {},
			mock: func(m *mock_manager_client.MockManagerClientMockRecorder) {
				m.GetSchedulerClusterConfig(gomock.Any(), gomock.Any()).Return(&manager.SchedulerConfig{
					ClusterId: "bar",
				}, nil).Times(1)
			},
			expect: func(t *testing.T, data *manager.SchedulerConfig, err error) {
				assert := assert.New(t)
				assert.Equal("bar", data.ClusterId)
			},
		},
		{
			name:   "client failed to return for the second time",
			expire: 10 * time.Millisecond,
			cleanFileCache: func(t *testing.T) {
				path, err := dc.DefaultCacheFile()
				if err != nil {
					t.Fatal(err)
				}

				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			},
			sleep: func() {
				time.Sleep(100 * time.Millisecond)
			},
			mock: func(m *mock_manager_client.MockManagerClientMockRecorder) {
				gomock.InOrder(
					m.GetSchedulerClusterConfig(gomock.Any(), gomock.Any()).Return(&manager.SchedulerConfig{
						ClusterId: "bar",
					}, nil).Times(1),
					m.GetSchedulerClusterConfig(gomock.Any(), gomock.Any()).Return(nil, errors.New("foo")).Times(1),
				)
			},
			expect: func(t *testing.T, data *manager.SchedulerConfig, err error) {
				assert := assert.New(t)
				assert.Equal("bar", data.ClusterId)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			mockManagerClient := mock_manager_client.NewMockManagerClient(ctl)
			tc.mock(mockManagerClient.EXPECT())

			d, err := NewDynconfig(mockManagerClient, tc.expire)
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
