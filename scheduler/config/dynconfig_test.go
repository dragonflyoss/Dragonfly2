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
	"path"
	"testing"
	"time"

	dc "d7y.io/dragonfly/v2/internal/dynconfig"
	"d7y.io/dragonfly/v2/pkg/rpc/manager"
	"d7y.io/dragonfly/v2/scheduler/config/mocks"
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
		mock           func(m *mocks.MockManagerClientMockRecorder)
		expect         func(t *testing.T, data *manager.Scheduler, err error)
	}{
		{
			name:   "get dynconfig success",
			expire: 10 * time.Second,
			cleanFileCache: func(t *testing.T) {
				if err := os.Remove(SchedulerDynconfigCachePath); err != nil {
					t.Fatal(err)
				}
			},
			sleep: func() {},
			mock: func(m *mocks.MockManagerClientMockRecorder) {
				m.GetScheduler(gomock.Any(), gomock.Any()).Return(&manager.Scheduler{
					Id: 1,
				}, nil).Times(1)
			},
			expect: func(t *testing.T, data *manager.Scheduler, err error) {
				assert := assert.New(t)
				assert.Equal(uint64(1), data.Id)
			},
		},
		{
			name:   "client failed to return for the second time",
			expire: 10 * time.Millisecond,
			cleanFileCache: func(t *testing.T) {
				if err := os.Remove(SchedulerDynconfigCachePath); err != nil {
					t.Fatal(err)
				}
			},
			sleep: func() {
				time.Sleep(100 * time.Millisecond)
			},
			mock: func(m *mocks.MockManagerClientMockRecorder) {
				gomock.InOrder(
					m.GetScheduler(gomock.Any(), gomock.Any()).Return(&manager.Scheduler{
						Id: 1,
					}, nil).Times(1),
					m.GetScheduler(gomock.Any(), gomock.Any()).Return(nil, errors.New("foo")).Times(1),
				)
			},
			expect: func(t *testing.T, data *manager.Scheduler, err error) {
				assert := assert.New(t)
				assert.Equal(uint64(1), data.Id)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			mockManagerClient := mocks.NewMockManagerClient(ctl)
			tc.mock(mockManagerClient.EXPECT())

			d, err := NewDynconfig(dc.ManagerSourceType, "", []dc.Option{
				dc.WithManagerClient(NewManagerClient(mockManagerClient)),
				dc.WithCachePath(SchedulerDynconfigCachePath),
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

func TestDynconfigGetCDNFromDirPath(t *testing.T) {
	tests := []struct {
		name       string
		cdnDirPath string
		expect     func(t *testing.T, data *manager.Scheduler, err error)
	}{
		{
			name:       "get CDN from directory",
			cdnDirPath: path.Join("./testdata", "cdn"),
			expect: func(t *testing.T, data *manager.Scheduler, err error) {
				assert := assert.New(t)
				assert.Equal([]*manager.CDN{
					{
						Id:           uint64(1),
						HostName:     "foo",
						Idc:          "foo",
						Location:     "foo",
						Ip:           "127.0.0.1",
						Port:         8003,
						DownloadPort: 8001,
						Status:       "active",
					},
					{
						Id:           uint64(2),
						HostName:     "bar",
						Idc:          "bar",
						Location:     "bar",
						Ip:           "127.0.0.1",
						Port:         8003,
						DownloadPort: 8001,
						Status:       "active",
					},
				}, data.Cdns)
			},
		},
		{
			name:       "directory does not exist",
			cdnDirPath: path.Join("./testdata", "foo"),
			expect: func(t *testing.T, data *manager.Scheduler, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "open testdata/foo: no such file or directory")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {

			d, err := NewDynconfig(dc.LocalSourceType, tc.cdnDirPath, []dc.Option{
				dc.WithLocalConfigPath(SchedulerDynconfigPath),
			}...)
			if err != nil {
				t.Fatal(err)
			}

			data, err := d.Get()
			tc.expect(t, data, err)
		})
	}
}
