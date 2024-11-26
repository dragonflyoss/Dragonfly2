/*
 *     Copyright 2022 The Dragonfly Authors
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

package announcer

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	managerv2 "d7y.io/api/v2/pkg/apis/manager/v2"

	managertypes "d7y.io/dragonfly/v2/manager/types"
	managerclientmocks "d7y.io/dragonfly/v2/pkg/rpc/manager/client/mocks"
	"d7y.io/dragonfly/v2/scheduler/config"
)

var (
	mockIDC      = "foo"
	mockLocation = "bar"
)

type mockReadCloserWithReadError struct{}

func (m *mockReadCloserWithReadError) Read(p []byte) (int, error) {
	return 0, errors.New("foo")
}

func (m *mockReadCloserWithReadError) Close() error {
	return nil
}

func TestAnnouncer_New(t *testing.T) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()

	tests := []struct {
		name   string
		config *config.Config
		mock   func(m *managerclientmocks.MockV2MockRecorder)
		expect func(t *testing.T, announcer Announcer, err error)
	}{
		{
			name: "new announcer",
			config: &config.Config{
				Server: config.ServerConfig{
					Host:          "localhost",
					AdvertiseIP:   net.ParseIP("127.0.0.1"),
					AdvertisePort: 8004,
					Port:          8080,
				},
				Host: config.HostConfig{
					IDC:      mockIDC,
					Location: mockLocation,
				},
				Manager: config.ManagerConfig{
					SchedulerClusterID: 1,
				},
			},
			mock: func(m *managerclientmocks.MockV2MockRecorder) {
				m.UpdateScheduler(gomock.Any(), gomock.Any()).Return(nil, nil).Times(1)
			},
			expect: func(t *testing.T, a Announcer, err error) {
				assert := assert.New(t)
				instance := a.(*announcer)
				assert.NoError(err)
				assert.NotNil(instance.config)
				assert.NotNil(instance.managerClient)
			},
		},
		{
			name: "update scheduler failed",
			config: &config.Config{
				Server: config.ServerConfig{
					Host:          "localhost",
					AdvertiseIP:   net.ParseIP("127.0.0.1"),
					AdvertisePort: 8004,
					Port:          8080,
				},
				Host: config.HostConfig{
					IDC:      mockIDC,
					Location: mockLocation,
				},
				Manager: config.ManagerConfig{
					SchedulerClusterID: 1,
				},
			},
			mock: func(m *managerclientmocks.MockV2MockRecorder) {
				m.UpdateScheduler(gomock.Any(), gomock.Any()).Return(nil, errors.New("foo")).Times(1)
			},
			expect: func(t *testing.T, a Announcer, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "foo")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockManagerClient := managerclientmocks.NewMockV2(ctl)
			tc.mock(mockManagerClient.EXPECT())

			a, err := New(tc.config, mockManagerClient, managertypes.DefaultSchedulerFeatures)
			tc.expect(t, a, err)
		})
	}
}

func TestAnnouncer_Serve(t *testing.T) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()

	tests := []struct {
		name   string
		config *config.Config
		data   []byte
		sleep  func()
		mock   func(data []byte, m *managerclientmocks.MockV2MockRecorder)
		except func(t *testing.T, a Announcer)
	}{
		{
			name: "started announcer server success",
			config: &config.Config{
				Server: config.ServerConfig{
					Host:          "localhost",
					AdvertiseIP:   net.ParseIP("127.0.0.1"),
					AdvertisePort: 8004,
					Port:          8080,
				},
				Host: config.HostConfig{
					IDC:      mockIDC,
					Location: mockLocation,
				},
				Manager: config.ManagerConfig{
					KeepAlive: config.KeepAliveConfig{
						Interval: 50 * time.Millisecond,
					},
					SchedulerClusterID: 1,
				},
			},
			data: []byte("bar"),
			sleep: func() {
				time.Sleep(3 * time.Second)
			},
			mock: func(data []byte, m *managerclientmocks.MockV2MockRecorder) {
				gomock.InOrder(
					m.UpdateScheduler(gomock.Any(), gomock.Eq(&managerv2.UpdateSchedulerRequest{
						SourceType:         managerv2.SourceType_SCHEDULER_SOURCE,
						Hostname:           "localhost",
						Ip:                 "127.0.0.1",
						Port:               int32(8004),
						Idc:                &mockIDC,
						Location:           &mockLocation,
						SchedulerClusterId: uint64(1),
						Features:           managertypes.DefaultSchedulerFeatures,
					})).Times(1),
					m.KeepAlive(gomock.Eq(50*time.Millisecond), gomock.Eq(&managerv2.KeepAliveRequest{
						SourceType: managerv2.SourceType_SCHEDULER_SOURCE,
						Hostname:   "localhost",
						Ip:         "127.0.0.1",
						ClusterId:  uint64(1),
					}), gomock.Any()).Times(1),
				)
			},
			except: func(t *testing.T, a Announcer) {
				go a.Serve()
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockManagerClient := managerclientmocks.NewMockV2(ctl)

			tc.mock(tc.data, mockManagerClient.EXPECT())
			a, err := New(tc.config, mockManagerClient, managertypes.DefaultSchedulerFeatures)
			if err != nil {
				t.Fatal(err)
			}

			tc.except(t, a)
			tc.sleep()
			a.Stop()
		})
	}
}

func TestAnnouncer_announceToManager(t *testing.T) {
	tests := []struct {
		name   string
		config *config.Config
		sleep  func()
		mock   func(m *managerclientmocks.MockV2MockRecorder)
		except func(a Announcer)
	}{
		{
			name: "announce to manager success",
			config: &config.Config{
				Server: config.ServerConfig{
					Host:          "localhost",
					AdvertiseIP:   net.ParseIP("127.0.0.1"),
					AdvertisePort: 8004,
					Port:          8080,
				},
				Host: config.HostConfig{
					IDC:      mockIDC,
					Location: mockLocation,
				},
				Manager: config.ManagerConfig{
					KeepAlive: config.KeepAliveConfig{
						Interval: 50 * time.Millisecond,
					},
					SchedulerClusterID: 1,
				},
			},
			sleep: func() {
				time.Sleep(100 * time.Millisecond)
			},
			mock: func(m *managerclientmocks.MockV2MockRecorder) {
				gomock.InOrder(
					m.UpdateScheduler(gomock.Any(), gomock.Eq(&managerv2.UpdateSchedulerRequest{
						SourceType:         managerv2.SourceType_SCHEDULER_SOURCE,
						Hostname:           "localhost",
						Ip:                 "127.0.0.1",
						Port:               int32(8004),
						Idc:                &mockIDC,
						Location:           &mockLocation,
						SchedulerClusterId: uint64(1),
						Features:           managertypes.DefaultSchedulerFeatures,
					})).Times(1),
					m.KeepAlive(gomock.Eq(50*time.Millisecond), gomock.Eq(&managerv2.KeepAliveRequest{
						SourceType: managerv2.SourceType_SCHEDULER_SOURCE,
						Hostname:   "localhost",
						Ip:         "127.0.0.1",
						ClusterId:  uint64(1),
					}), gomock.Any()).Times(1),
				)
			},
			except: func(a Announcer) {
				a.(*announcer).announceToManager()
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			mockManagerClient := managerclientmocks.NewMockV2(ctl)
			tc.mock(mockManagerClient.EXPECT())

			a, err := New(tc.config, mockManagerClient, managertypes.DefaultSchedulerFeatures)
			if err != nil {
				t.Fatal(err)
			}

			tc.except(a)
			tc.sleep()
		})
	}
}
