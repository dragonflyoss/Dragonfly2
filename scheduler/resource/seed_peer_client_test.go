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

package resource

import (
	"errors"
	"fmt"
	"testing"

	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"

	managerv1 "d7y.io/api/pkg/apis/manager/v1"

	"d7y.io/dragonfly/v2/pkg/dfnet"
	pkgtypes "d7y.io/dragonfly/v2/pkg/types"
	"d7y.io/dragonfly/v2/scheduler/config"
	configmocks "d7y.io/dragonfly/v2/scheduler/config/mocks"
)

func TestSeedPeerClient_newSeedPeerClient(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(dynconfig *configmocks.MockDynconfigInterfaceMockRecorder, hostManager *MockHostManagerMockRecorder)
		expect func(t *testing.T, err error)
	}{
		{
			name: "new seed peer client",
			mock: func(dynconfig *configmocks.MockDynconfigInterfaceMockRecorder, hostManager *MockHostManagerMockRecorder) {
				gomock.InOrder(
					dynconfig.Get().Return(&config.DynconfigData{
						Scheduler: &managerv1.Scheduler{
							SeedPeers: []*managerv1.SeedPeer{{Id: 1}},
						},
					}, nil).Times(1),
					dynconfig.Register(gomock.Any()).Return().Times(1),
					dynconfig.GetResolveSeedPeerAddrs().Return([]resolver.Address{}, nil).Times(1),
					hostManager.Load(gomock.Any()).Return(nil, false).Times(1),
					hostManager.Store(gomock.Any()).Return().Times(1),
					dynconfig.Register(gomock.Any()).Return().Times(1),
				)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
		{
			name: "new seed peer client failed because of dynconfig get error data",
			mock: func(dynconfig *configmocks.MockDynconfigInterfaceMockRecorder, hostManager *MockHostManagerMockRecorder) {
				dynconfig.Get().Return(&config.DynconfigData{}, errors.New("foo")).Times(1)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "foo")
			},
		},
		{
			name: "new seed peer client failed because of seed peer list is empty",
			mock: func(dynconfig *configmocks.MockDynconfigInterfaceMockRecorder, hostManager *MockHostManagerMockRecorder) {
				gomock.InOrder(
					dynconfig.Get().Return(&config.DynconfigData{
						Scheduler: &managerv1.Scheduler{
							SeedPeers: []*managerv1.SeedPeer{},
						},
					}, nil).Times(1),
					dynconfig.Register(gomock.Any()).Return().Times(1),
					dynconfig.GetResolveSeedPeerAddrs().Return([]resolver.Address{}, nil).Times(1),
					dynconfig.Register(gomock.Any()).Return().Times(1),
				)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			hostManager := NewMockHostManager(ctl)
			tc.mock(dynconfig.EXPECT(), hostManager.EXPECT())

			_, err := newSeedPeerClient(dynconfig, hostManager, grpc.WithTransportCredentials(insecure.NewCredentials()))
			tc.expect(t, err)
		})
	}
}

func TestSeedPeerClient_OnNotify(t *testing.T) {
	tests := []struct {
		name string
		data *config.DynconfigData
		mock func(dynconfig *configmocks.MockDynconfigInterfaceMockRecorder, hostManager *MockHostManagerMockRecorder)
	}{
		{
			name: "notify client without different seedPeers",
			data: &config.DynconfigData{
				Scheduler: &managerv1.Scheduler{
					SeedPeers: []*managerv1.SeedPeer{
						{
							Id:       1,
							HostName: "foo",
							Ip:       "0.0.0.0",
							Port:     8080,
						},
					},
				},
			},
			mock: func(dynconfig *configmocks.MockDynconfigInterfaceMockRecorder, hostManager *MockHostManagerMockRecorder) {
				gomock.InOrder(
					dynconfig.Get().Return(&config.DynconfigData{
						Scheduler: &managerv1.Scheduler{
							SeedPeers: []*managerv1.SeedPeer{
								{
									Id:       1,
									HostName: "foo",
									Ip:       "0.0.0.0",
									Port:     8080,
								},
							},
						},
					}, nil).Times(1),
					dynconfig.Register(gomock.Any()).Return().Times(1),
					dynconfig.GetResolveSeedPeerAddrs().Return([]resolver.Address{}, nil).Times(1),
					hostManager.Load(gomock.Any()).Return(nil, false).Times(1),
					hostManager.Store(gomock.Any()).Return().Times(1),
					dynconfig.Register(gomock.Any()).Return().Times(1),
				)
			},
		},
		{
			name: "notify client with different seedPeers",
			data: &config.DynconfigData{
				Scheduler: &managerv1.Scheduler{
					SeedPeers: []*managerv1.SeedPeer{
						{
							Id:       1,
							HostName: "foo",
							Ip:       "0.0.0.0",
						},
					},
				},
			},
			mock: func(dynconfig *configmocks.MockDynconfigInterfaceMockRecorder, hostManager *MockHostManagerMockRecorder) {
				mockHost := NewHost(mockRawHost)
				gomock.InOrder(
					dynconfig.Get().Return(&config.DynconfigData{
						Scheduler: &managerv1.Scheduler{
							SeedPeers: []*managerv1.SeedPeer{
								{
									Id:       1,
									HostName: "foo",
									Ip:       "127.0.0.1",
								},
							},
						},
					}, nil).Times(1),
					dynconfig.Register(gomock.Any()).Return().Times(1),
					dynconfig.GetResolveSeedPeerAddrs().Return([]resolver.Address{}, nil).Times(1),
					hostManager.Load(gomock.Any()).Return(nil, false).Times(1),
					hostManager.Store(gomock.Any()).Return().Times(1),
					dynconfig.Register(gomock.Any()).Return().Times(1),
					hostManager.Load(gomock.Any()).Return(mockHost, true).Times(1),
					hostManager.Delete(gomock.Eq("foo-0")).Return().Times(1),
					hostManager.Load(gomock.Any()).Return(mockHost, true).Times(1),
				)
			},
		},
		{
			name: "notify client with different seed peers and load host failed",
			data: &config.DynconfigData{
				Scheduler: &managerv1.Scheduler{
					SeedPeers: []*managerv1.SeedPeer{
						{
							Id:       1,
							HostName: "foo",
							Ip:       "0.0.0.0",
						},
					},
				},
			},
			mock: func(dynconfig *configmocks.MockDynconfigInterfaceMockRecorder, hostManager *MockHostManagerMockRecorder) {
				gomock.InOrder(
					dynconfig.Get().Return(&config.DynconfigData{
						Scheduler: &managerv1.Scheduler{
							SeedPeers: []*managerv1.SeedPeer{
								{
									Id:       1,
									HostName: "foo",
									Ip:       "127.0.0.1",
								},
							},
						},
					}, nil).Times(1),
					dynconfig.Register(gomock.Any()).Return().Times(1),
					dynconfig.GetResolveSeedPeerAddrs().Return([]resolver.Address{}, nil).Times(1),
					hostManager.Load(gomock.Any()).Return(nil, false).Times(1),
					hostManager.Store(gomock.Any()).Return().Times(1),
					dynconfig.Register(gomock.Any()).Return().Times(1),
					hostManager.Load(gomock.Any()).Return(nil, false).Times(1),
					hostManager.Load(gomock.Any()).Return(nil, false).Times(1),
					hostManager.Store(gomock.Any()).Return().Times(1),
				)
			},
		},
		{
			name: "seed peer list is deep equal",
			data: &config.DynconfigData{
				Scheduler: &managerv1.Scheduler{
					SeedPeers: []*managerv1.SeedPeer{
						{
							Id: 1,
							Ip: "127.0.0.1",
						},
					},
				},
			},
			mock: func(dynconfig *configmocks.MockDynconfigInterfaceMockRecorder, hostManager *MockHostManagerMockRecorder) {
				gomock.InOrder(
					dynconfig.Get().Return(&config.DynconfigData{
						Scheduler: &managerv1.Scheduler{
							SeedPeers: []*managerv1.SeedPeer{
								{
									Id: 1,
									Ip: "127.0.0.1",
								},
							},
						},
					}, nil).Times(1),
					dynconfig.Register(gomock.Any()).Return().Times(1),
					dynconfig.GetResolveSeedPeerAddrs().Return([]resolver.Address{}, nil).Times(1),
					hostManager.Load(gomock.Any()).Return(nil, false).Times(1),
					hostManager.Store(gomock.Any()).Return().Times(1),
					dynconfig.Register(gomock.Any()).Return().Times(1),
				)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			hostManager := NewMockHostManager(ctl)
			tc.mock(dynconfig.EXPECT(), hostManager.EXPECT())

			client, err := newSeedPeerClient(dynconfig, hostManager, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}
			client.OnNotify(tc.data)
		})
	}
}

func TestSeedPeerClient_seedPeersToNetAddrs(t *testing.T) {
	tests := []struct {
		name      string
		seedPeers []*managerv1.SeedPeer
		expect    func(t *testing.T, netAddrs []dfnet.NetAddr)
	}{
		{
			name: "seed peers covert to netAddr",
			seedPeers: []*managerv1.SeedPeer{
				{
					Id:           1,
					Type:         pkgtypes.HostTypeSuperSeedName,
					HostName:     mockRawSeedHost.Hostname,
					Ip:           mockRawSeedHost.Ip,
					Port:         mockRawSeedHost.Port,
					DownloadPort: mockRawSeedHost.DownloadPort,
					Idc:          mockRawSeedHost.Network.Idc,
					NetTopology:  mockRawSeedHost.Network.NetTopology,
					Location:     mockRawSeedHost.Network.Location,
				},
			},
			expect: func(t *testing.T, netAddrs []dfnet.NetAddr) {
				assert := assert.New(t)
				assert.Equal(netAddrs[0].Type, dfnet.TCP)
				assert.Equal(netAddrs[0].Addr, fmt.Sprintf("%s:%d", mockRawSeedHost.Ip, mockRawSeedHost.Port))
			},
		},
		{
			name:      "seed peers is empty",
			seedPeers: []*managerv1.SeedPeer{},
			expect: func(t *testing.T, netAddrs []dfnet.NetAddr) {
				assert := assert.New(t)
				assert.Equal(len(netAddrs), 0)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, seedPeersToNetAddrs(tc.seedPeers))
		})
	}
}

func TestSeedPeerClient_diffSeedPeers(t *testing.T) {
	tests := []struct {
		name   string
		sx     []*managerv1.SeedPeer
		sy     []*managerv1.SeedPeer
		expect func(t *testing.T, diff []*managerv1.SeedPeer)
	}{
		{
			name: "same seed peer list",
			sx: []*managerv1.SeedPeer{
				{
					Id:       1,
					HostName: "foo",
					Ip:       "127.0.0.1",
					Port:     8080,
				},
			},
			sy: []*managerv1.SeedPeer{
				{
					Id:       1,
					HostName: "foo",
					Ip:       "127.0.0.1",
					Port:     8080,
				},
			},
			expect: func(t *testing.T, diff []*managerv1.SeedPeer) {
				assert := assert.New(t)
				assert.EqualValues(diff, []*managerv1.SeedPeer(nil))
			},
		},
		{
			name: "different hostname",
			sx: []*managerv1.SeedPeer{
				{
					Id:       1,
					HostName: "bar",
					Ip:       "127.0.0.1",
					Port:     8080,
				},
			},
			sy: []*managerv1.SeedPeer{
				{
					Id:       1,
					HostName: "foo",
					Ip:       "127.0.0.1",
					Port:     8080,
				},
			},
			expect: func(t *testing.T, diff []*managerv1.SeedPeer) {
				assert := assert.New(t)
				assert.EqualValues(diff, []*managerv1.SeedPeer{
					{
						Id:       1,
						HostName: "bar",
						Ip:       "127.0.0.1",
						Port:     8080,
					},
				})
			},
		},
		{
			name: "different port",
			sx: []*managerv1.SeedPeer{
				{
					Id:       1,
					HostName: "foo",
					Ip:       "127.0.0.1",
					Port:     8081,
				},
			},
			sy: []*managerv1.SeedPeer{
				{
					Id:       1,
					HostName: "foo",
					Ip:       "127.0.0.1",
					Port:     8080,
				},
			},
			expect: func(t *testing.T, diff []*managerv1.SeedPeer) {
				assert := assert.New(t)
				assert.EqualValues(diff, []*managerv1.SeedPeer{
					{
						Id:       1,
						HostName: "foo",
						Ip:       "127.0.0.1",
						Port:     8081,
					},
				})
			},
		},
		{
			name: "different ip",
			sx: []*managerv1.SeedPeer{
				{
					Id:       1,
					HostName: "foo",
					Ip:       "0.0.0.0",
					Port:     8080,
				},
			},
			sy: []*managerv1.SeedPeer{
				{
					Id:       1,
					HostName: "foo",
					Ip:       "127.0.0.1",
					Port:     8080,
				},
			},
			expect: func(t *testing.T, diff []*managerv1.SeedPeer) {
				assert := assert.New(t)
				assert.EqualValues(diff, []*managerv1.SeedPeer{
					{
						Id:       1,
						HostName: "foo",
						Ip:       "0.0.0.0",
						Port:     8080,
					},
				})
			},
		},
		{
			name: "remove y seed peer",
			sx: []*managerv1.SeedPeer{
				{
					Id:       1,
					HostName: "foo",
					Ip:       "127.0.0.1",
					Port:     8080,
				},
				{
					Id:       2,
					HostName: "bar",
					Ip:       "127.0.0.1",
					Port:     8080,
				},
			},
			sy: []*managerv1.SeedPeer{
				{
					Id:       1,
					HostName: "foo",
					Ip:       "127.0.0.1",
					Port:     8080,
				},
			},
			expect: func(t *testing.T, diff []*managerv1.SeedPeer) {
				assert := assert.New(t)
				assert.EqualValues(diff, []*managerv1.SeedPeer{
					{
						Id:       2,
						HostName: "bar",
						Ip:       "127.0.0.1",
						Port:     8080,
					},
				})
			},
		},
		{
			name: "remove x seed peer",
			sx: []*managerv1.SeedPeer{
				{
					Id:       1,
					HostName: "foo",
					Ip:       "127.0.0.1",
					Port:     8080,
				},
			},
			sy: []*managerv1.SeedPeer{
				{
					Id:       1,
					HostName: "baz",
					Ip:       "127.0.0.1",
					Port:     8080,
				},
				{
					Id:       2,
					HostName: "bar",
					Ip:       "127.0.0.1",
					Port:     8080,
				},
			},
			expect: func(t *testing.T, diff []*managerv1.SeedPeer) {
				assert := assert.New(t)
				assert.EqualValues(diff, []*managerv1.SeedPeer{
					{
						Id:       1,
						HostName: "foo",
						Ip:       "127.0.0.1",
						Port:     8080,
					},
				})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, diffSeedPeers(tc.sx, tc.sy))
		})
	}
}
