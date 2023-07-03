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

package resource

import (
	"errors"
	"reflect"
	"testing"
	"time"

	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	commonv2 "d7y.io/api/pkg/apis/common/v2"

	"d7y.io/dragonfly/v2/pkg/gc"
	"d7y.io/dragonfly/v2/scheduler/config"
)

var (
	mockHostGCConfig = &config.GCConfig{
		HostGCInterval: 1 * time.Second,
	}
)

func TestHostManager_newHostManager(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(m *gc.MockGCMockRecorder)
		expect func(t *testing.T, hostManager HostManager, err error)
	}{
		{
			name: "new host manager",
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, hostManager HostManager, err error) {
				assert := assert.New(t)
				assert.Equal(reflect.TypeOf(hostManager).Elem().Name(), "hostManager")
			},
		},
		{
			name: "new host manager failed because of gc error",
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(errors.New("foo")).Times(1)
			},
			expect: func(t *testing.T, hostManager HostManager, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "foo")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			gc := gc.NewMockGC(ctl)
			tc.mock(gc.EXPECT())
			hostManager, err := newHostManager(mockHostGCConfig, gc)

			tc.expect(t, hostManager, err)
		})
	}
}

func TestHostManager_Load(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(m *gc.MockGCMockRecorder)
		expect func(t *testing.T, hostManager HostManager, mockHost *Host)
	}{
		{
			name: "load host",
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, hostManager HostManager, mockHost *Host) {
				assert := assert.New(t)
				hostManager.Store(mockHost)
				host, loaded := hostManager.Load(mockHost.ID)
				assert.Equal(loaded, true)
				assert.Equal(host.ID, mockHost.ID)
			},
		},
		{
			name: "host does not exist",
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, hostManager HostManager, mockHost *Host) {
				assert := assert.New(t)
				_, loaded := hostManager.Load(mockHost.ID)
				assert.Equal(loaded, false)
			},
		},
		{
			name: "load key is empty",
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, hostManager HostManager, mockHost *Host) {
				assert := assert.New(t)
				mockHost.ID = ""
				hostManager.Store(mockHost)
				host, loaded := hostManager.Load(mockHost.ID)
				assert.Equal(loaded, true)
				assert.Equal(host.ID, mockHost.ID)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			gc := gc.NewMockGC(ctl)
			tc.mock(gc.EXPECT())

			mockHost := NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			hostManager, err := newHostManager(mockHostGCConfig, gc)
			if err != nil {
				t.Fatal(err)
			}

			tc.expect(t, hostManager, mockHost)
		})
	}
}

func TestHostManager_Store(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(m *gc.MockGCMockRecorder)
		expect func(t *testing.T, hostManager HostManager, mockHost *Host)
	}{
		{
			name: "store host",
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, hostManager HostManager, mockHost *Host) {
				assert := assert.New(t)
				hostManager.Store(mockHost)
				host, loaded := hostManager.Load(mockHost.ID)
				assert.Equal(loaded, true)
				assert.Equal(host.ID, mockHost.ID)
			},
		},
		{
			name: "store key is empty",
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, hostManager HostManager, mockHost *Host) {
				assert := assert.New(t)
				mockHost.ID = ""
				hostManager.Store(mockHost)
				host, loaded := hostManager.Load(mockHost.ID)
				assert.Equal(loaded, true)
				assert.Equal(host.ID, mockHost.ID)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			gc := gc.NewMockGC(ctl)
			tc.mock(gc.EXPECT())

			mockHost := NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			hostManager, err := newHostManager(mockHostGCConfig, gc)
			if err != nil {
				t.Fatal(err)
			}

			tc.expect(t, hostManager, mockHost)
		})
	}
}

func TestHostManager_LoadOrStore(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(m *gc.MockGCMockRecorder)
		expect func(t *testing.T, hostManager HostManager, mockHost *Host)
	}{
		{
			name: "load host exist",
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, hostManager HostManager, mockHost *Host) {
				assert := assert.New(t)
				hostManager.Store(mockHost)
				host, loaded := hostManager.LoadOrStore(mockHost)
				assert.Equal(loaded, true)
				assert.Equal(host.ID, mockHost.ID)
			},
		},
		{
			name: "load host does not exist",
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, hostManager HostManager, mockHost *Host) {
				assert := assert.New(t)
				host, loaded := hostManager.LoadOrStore(mockHost)
				assert.Equal(loaded, false)
				assert.Equal(host.ID, mockHost.ID)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			gc := gc.NewMockGC(ctl)
			tc.mock(gc.EXPECT())

			mockHost := NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			hostManager, err := newHostManager(mockHostGCConfig, gc)
			if err != nil {
				t.Fatal(err)
			}

			tc.expect(t, hostManager, mockHost)
		})
	}
}

func TestHostManager_Delete(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(m *gc.MockGCMockRecorder)
		expect func(t *testing.T, hostManager HostManager, mockHost *Host)
	}{
		{
			name: "delete host",
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, hostManager HostManager, mockHost *Host) {
				assert := assert.New(t)
				hostManager.Store(mockHost)
				hostManager.Delete(mockHost.ID)
				_, loaded := hostManager.Load(mockHost.ID)
				assert.Equal(loaded, false)
			},
		},
		{
			name: "delete key does not exist",
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, hostManager HostManager, mockHost *Host) {
				assert := assert.New(t)
				mockHost.ID = ""
				hostManager.Store(mockHost)
				hostManager.Delete(mockHost.ID)
				_, loaded := hostManager.Load(mockHost.ID)
				assert.Equal(loaded, false)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			gc := gc.NewMockGC(ctl)
			tc.mock(gc.EXPECT())

			mockHost := NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			hostManager, err := newHostManager(mockHostGCConfig, gc)
			if err != nil {
				t.Fatal(err)
			}

			tc.expect(t, hostManager, mockHost)
		})
	}
}

func TestHostManager_GetDestHosts(t *testing.T) {
	tests := []struct {
		name   string
		hosts  []*Host
		mock   func(m *gc.MockGCMockRecorder)
		expect func(t *testing.T, hostManager HostManager, hosts []*Host)
	}{
		{
			name: "get host ids",
			hosts: []*Host{
				NewHost(
					mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
					mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type),
				NewHost(
					mockRawSeedHost.ID, mockRawSeedHost.IP, mockRawSeedHost.Hostname,
					mockRawSeedHost.Port, mockRawSeedHost.DownloadPort, mockRawSeedHost.Type),
			},
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, hostManager HostManager, hosts []*Host) {
				assert := assert.New(t)
				for _, host := range hosts {
					hostManager.Store(host)
				}

				h := hostManager.GetDestHosts(mockRawSeedHost.ID, 2)
				assert.Equal(len(h), 1)
			},
		},
		{
			name: "require 0 host id",
			hosts: []*Host{
				NewHost(
					mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
					mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type),
				NewHost(
					mockRawSeedHost.ID, mockRawSeedHost.IP, mockRawSeedHost.Hostname,
					mockRawSeedHost.Port, mockRawSeedHost.DownloadPort, mockRawSeedHost.Type),
			},
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, hostManager HostManager, hosts []*Host) {
				assert := assert.New(t)
				for _, host := range hosts {
					hostManager.Store(host)
				}

				h := hostManager.GetDestHosts(mockRawSeedHost.ID, 0)
				assert.Equal(len(h), 0)
			},
		},
		{
			name:  "map is empty",
			hosts: []*Host{},
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, hostManager HostManager, hosts []*Host) {
				assert := assert.New(t)
				for _, host := range hosts {
					hostManager.Store(host)
				}

				h := hostManager.GetDestHosts(mockRawSeedHost.ID, 1)
				assert.Equal(len(h), 0)
			},
		},
		{
			name: "the number of hosts in the map is insufficient",
			hosts: []*Host{
				NewHost(
					mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
					mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type),
				NewHost(
					mockRawSeedHost.ID, mockRawSeedHost.IP, mockRawSeedHost.Hostname,
					mockRawSeedHost.Port, mockRawSeedHost.DownloadPort, mockRawSeedHost.Type),
			},
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, hostManager HostManager, hosts []*Host) {
				assert := assert.New(t)
				for _, host := range hosts {
					hostManager.Store(host)
				}

				h := hostManager.GetDestHosts(mockRawSeedHost.ID, 3)
				assert.Equal(len(h), 1)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			gc := gc.NewMockGC(ctl)
			tc.mock(gc.EXPECT())

			hostManager, err := newHostManager(mockHostGCConfig, gc)
			if err != nil {
				t.Fatal(err)
			}

			tc.expect(t, hostManager, tc.hosts)
		})
	}
}

func TestHostManager_RunGC(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(m *gc.MockGCMockRecorder)
		expect func(t *testing.T, hostManager HostManager, mockHost *Host, mockPeer *Peer)
	}{
		{
			name: "host reclaimed",
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, hostManager HostManager, mockHost *Host, mockPeer *Peer) {
				assert := assert.New(t)
				hostManager.Store(mockHost)
				err := hostManager.RunGC()
				assert.NoError(err)

				_, loaded := hostManager.Load(mockHost.ID)
				assert.Equal(loaded, false)
			},
		},
		{
			name: "host has peers",
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, hostManager HostManager, mockHost *Host, mockPeer *Peer) {
				assert := assert.New(t)
				hostManager.Store(mockHost)
				mockHost.StorePeer(mockPeer)
				err := hostManager.RunGC()
				assert.NoError(err)

				host, loaded := hostManager.Load(mockHost.ID)
				assert.Equal(loaded, true)
				assert.Equal(host.ID, mockHost.ID)
			},
		},
		{
			name: "host has upload peers",
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, hostManager HostManager, mockHost *Host, mockPeer *Peer) {
				assert := assert.New(t)
				hostManager.Store(mockHost)
				mockHost.StorePeer(mockPeer)
				mockHost.PeerCount.Add(0)
				mockHost.ConcurrentUploadCount.Add(1)
				err := hostManager.RunGC()
				assert.NoError(err)

				host, loaded := hostManager.Load(mockHost.ID)
				assert.Equal(loaded, true)
				assert.Equal(host.ID, mockHost.ID)
			},
		},
		{
			name: "host is seed peer",
			mock: func(m *gc.MockGCMockRecorder) {
				m.Add(gomock.Any()).Return(nil).Times(1)
			},
			expect: func(t *testing.T, hostManager HostManager, mockHost *Host, mockPeer *Peer) {
				assert := assert.New(t)
				mockSeedHost := NewHost(
					mockRawSeedHost.ID, mockRawSeedHost.IP, mockRawSeedHost.Hostname,
					mockRawSeedHost.Port, mockRawSeedHost.DownloadPort, mockRawSeedHost.Type)
				hostManager.Store(mockSeedHost)
				err := hostManager.RunGC()
				assert.NoError(err)

				host, loaded := hostManager.Load(mockSeedHost.ID)
				assert.Equal(loaded, true)
				assert.Equal(host.ID, mockSeedHost.ID)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			gc := gc.NewMockGC(ctl)
			tc.mock(gc.EXPECT())

			mockHost := NewHost(
				mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
				mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
			mockTask := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilters, mockTaskHeader, mockTaskBackToSourceLimit)
			mockPeer := NewPeer(mockPeerID, mockTask, mockHost)
			hostManager, err := newHostManager(mockHostGCConfig, gc)
			if err != nil {
				t.Fatal(err)
			}

			tc.expect(t, hostManager, mockHost, mockPeer)
		})
	}
}
