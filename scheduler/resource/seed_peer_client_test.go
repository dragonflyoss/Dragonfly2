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

	"github.com/stretchr/testify/assert"
	gomock "go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"

	managerv2 "d7y.io/api/v2/pkg/apis/manager/v2"

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
						Scheduler: &managerv2.Scheduler{
							SeedPeers: []*managerv2.SeedPeer{{Id: 1}},
						},
					}, nil).Times(1),
					dynconfig.Register(gomock.Any()).Return().Times(1),
					dynconfig.GetResolveSeedPeerAddrs().Return([]resolver.Address{}, nil).Times(1),
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
						Scheduler: &managerv2.Scheduler{
							SeedPeers: []*managerv2.SeedPeer{},
						},
					}, nil).Times(1),
					dynconfig.Register(gomock.Any()).Return().Times(1),
					dynconfig.GetResolveSeedPeerAddrs().Return([]resolver.Address{}, nil).Times(1),
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

func TestSeedPeerClinet_Addrs(t *testing.T) {
	tests := []struct {
		name   string
		sc     *seedPeerClient
		expect func(t *testing.T, addrs []string)
	}{
		{
			name: "return the addresses of seed peers",
			sc: &seedPeerClient{
				data: &config.DynconfigData{
					Scheduler: &managerv2.Scheduler{
						SeedPeers: []*managerv2.SeedPeer{
							{
								Ip:   "0.0.0.0",
								Port: 8080,
							},
							{
								Ip:   "127.0.0.1",
								Port: 5000,
							},
						},
					},
				},
			},
			expect: func(t *testing.T, addrs []string) {
				assert := assert.New(t)
				assert.Equal("0.0.0.0:8080", addrs[0])
				assert.Equal("127.0.0.1:5000", addrs[1])
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			addrs := tc.sc.Addrs()
			tc.expect(t, addrs)
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
			name: "notify client with same seed peers",
			data: &config.DynconfigData{
				Scheduler: &managerv2.Scheduler{
					SeedPeers: []*managerv2.SeedPeer{
						{
							Id:       1,
							Hostname: "foo",
							Ip:       "0.0.0.0",
							Port:     8080,
						},
					},
				},
			},
			mock: func(dynconfig *configmocks.MockDynconfigInterfaceMockRecorder, hostManager *MockHostManagerMockRecorder) {
				gomock.InOrder(
					dynconfig.Get().Return(&config.DynconfigData{
						Scheduler: &managerv2.Scheduler{
							SeedPeers: []*managerv2.SeedPeer{
								{
									Id:       1,
									Hostname: "foo",
									Ip:       "0.0.0.0",
									Port:     8080,
								},
							},
						},
					}, nil).Times(1),
					dynconfig.Register(gomock.Any()).Return().Times(1),
					dynconfig.GetResolveSeedPeerAddrs().Return([]resolver.Address{}, nil).Times(1),
					dynconfig.Register(gomock.Any()).Return().Times(1),
					dynconfig.GetResolveSeedPeerAddrs().Return([]resolver.Address{}, nil).Times(1),
					hostManager.Load(gomock.Any()).Return(nil, false).Times(1),
					hostManager.Store(gomock.Any()).Return().Times(1),
					dynconfig.Register(gomock.Any()).Return().Times(1),
				)
			},
		},
		{
			name: "notify client with different seed peers",
			data: &config.DynconfigData{
				Scheduler: &managerv2.Scheduler{
					SeedPeers: []*managerv2.SeedPeer{
						{
							Id:       1,
							Hostname: "foo",
							Ip:       "0.0.0.0",
						},
					},
				},
			},
			mock: func(dynconfig *configmocks.MockDynconfigInterfaceMockRecorder, hostManager *MockHostManagerMockRecorder) {
				mockHost := NewHost(
					mockRawHost.ID, mockRawHost.IP, mockRawHost.Hostname,
					mockRawHost.Port, mockRawHost.DownloadPort, mockRawHost.Type)
				gomock.InOrder(
					dynconfig.Get().Return(&config.DynconfigData{
						Scheduler: &managerv2.Scheduler{
							SeedPeers: []*managerv2.SeedPeer{
								{
									Id:       1,
									Hostname: "foo",
									Ip:       "127.0.0.1",
								},
							},
						},
					}, nil).Times(1),
					dynconfig.Register(gomock.Any()).Return().Times(1),
					dynconfig.GetResolveSeedPeerAddrs().Return([]resolver.Address{}, nil).Times(1),
					dynconfig.Register(gomock.Any()).Return().Times(1),
					dynconfig.GetResolveSeedPeerAddrs().Return([]resolver.Address{}, nil).Times(1),
					hostManager.Load(gomock.Any()).Return(nil, false).Times(1),
					hostManager.Store(gomock.Any()).Return().Times(1),
					dynconfig.Register(gomock.Any()).Return().Times(1),
					hostManager.Load(gomock.Any()).Return(mockHost, true).Times(1),
				)
			},
		},
		{
			name: "notify client with different seed peers and load host failed",
			data: &config.DynconfigData{
				Scheduler: &managerv2.Scheduler{
					SeedPeers: []*managerv2.SeedPeer{
						{
							Id:       1,
							Hostname: "foo",
							Ip:       "0.0.0.0",
						},
					},
				},
			},
			mock: func(dynconfig *configmocks.MockDynconfigInterfaceMockRecorder, hostManager *MockHostManagerMockRecorder) {
				gomock.InOrder(
					dynconfig.Get().Return(&config.DynconfigData{
						Scheduler: &managerv2.Scheduler{
							SeedPeers: []*managerv2.SeedPeer{
								{
									Id:       1,
									Hostname: "foo",
									Ip:       "127.0.0.1",
								},
							},
						},
					}, nil).Times(1),
					dynconfig.Register(gomock.Any()).Return().Times(1),
					dynconfig.GetResolveSeedPeerAddrs().Return([]resolver.Address{}, nil).Times(1),
					dynconfig.Register(gomock.Any()).Return().Times(1),
					dynconfig.GetResolveSeedPeerAddrs().Return([]resolver.Address{}, nil).Times(1),
					hostManager.Load(gomock.Any()).Return(nil, false).Times(1),
					hostManager.Store(gomock.Any()).Return().Times(1),
					dynconfig.Register(gomock.Any()).Return().Times(1),
					hostManager.Load(gomock.Any()).Return(nil, false).Times(1),
					hostManager.Store(gomock.Any()).Return().Times(1),
				)
			},
		},
		{
			name: "seed peer list is deep equal",
			data: &config.DynconfigData{
				Scheduler: &managerv2.Scheduler{
					SeedPeers: []*managerv2.SeedPeer{
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
						Scheduler: &managerv2.Scheduler{
							SeedPeers: []*managerv2.SeedPeer{
								{
									Id: 1,
									Ip: "127.0.0.1",
								},
							},
						},
					}, nil).Times(1),
					dynconfig.Register(gomock.Any()).Return().Times(1),
					dynconfig.GetResolveSeedPeerAddrs().Return([]resolver.Address{}, nil).Times(1),
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
		seedPeers []*managerv2.SeedPeer
		expect    func(t *testing.T, netAddrs []dfnet.NetAddr)
	}{
		{
			name: "seed peers covert to netAddr",
			seedPeers: []*managerv2.SeedPeer{
				{
					Id:           1,
					Type:         pkgtypes.HostTypeSuperSeedName,
					Hostname:     mockRawSeedHost.Hostname,
					Ip:           mockRawSeedHost.IP,
					Port:         mockRawSeedHost.Port,
					DownloadPort: mockRawSeedHost.DownloadPort,
					Idc:          &mockRawSeedHost.Network.IDC,
					Location:     &mockRawSeedHost.Network.Location,
				},
			},
			expect: func(t *testing.T, netAddrs []dfnet.NetAddr) {
				assert := assert.New(t)
				assert.Equal(netAddrs[0].Type, dfnet.TCP)
				assert.Equal(netAddrs[0].Addr, fmt.Sprintf("%s:%d", mockRawSeedHost.IP, mockRawSeedHost.Port))
			},
		},
		{
			name:      "seed peers is empty",
			seedPeers: []*managerv2.SeedPeer{},
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
