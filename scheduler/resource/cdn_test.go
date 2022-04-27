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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"

	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/manager/types"
	"d7y.io/dragonfly/v2/pkg/dfnet"
	rpcscheduler "d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/scheduler/config"
	configmocks "d7y.io/dragonfly/v2/scheduler/config/mocks"
)

func TestCDN_newCDN(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, cdn CDN)
	}{
		{
			name: "new cdn",
			expect: func(t *testing.T, cdn CDN) {
				assert := assert.New(t)
				assert.Equal(reflect.TypeOf(cdn).Elem().Name(), "cdn")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			hostManager := NewMockHostManager(ctl)
			peerManager := NewMockPeerManager(ctl)
			client := NewMockCDNClient(ctl)

			tc.expect(t, newCDN(peerManager, hostManager, client))
		})
	}
}

func TestCDN_TriggerTask(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(mc *MockCDNClientMockRecorder)
		expect func(t *testing.T, peer *Peer, result *rpcscheduler.PeerResult, err error)
	}{
		{
			name: "start obtain seed stream failed",
			mock: func(mc *MockCDNClientMockRecorder) {
				mc.ObtainSeeds(gomock.Any(), gomock.Any()).Return(nil, errors.New("foo")).Times(1)
			},
			expect: func(t *testing.T, peer *Peer, result *rpcscheduler.PeerResult, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "foo")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			hostManager := NewMockHostManager(ctl)
			peerManager := NewMockPeerManager(ctl)
			client := NewMockCDNClient(ctl)
			tc.mock(client.EXPECT())

			cdn := newCDN(peerManager, hostManager, client)
			mockTask := NewTask(mockTaskID, mockTaskURL, TaskTypeNormal, mockTaskURLMeta, WithBackToSourceLimit(mockTaskBackToSourceLimit))
			peer, result, err := cdn.TriggerTask(context.Background(), mockTask)
			tc.expect(t, peer, result, err)
		})
	}
}

func TestCDNClient_newCDNClient(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(dynconfig *configmocks.MockDynconfigInterfaceMockRecorder, hostManager *MockHostManagerMockRecorder)
		expect func(t *testing.T, err error)
	}{
		{
			name: "new cdn client",
			mock: func(dynconfig *configmocks.MockDynconfigInterfaceMockRecorder, hostManager *MockHostManagerMockRecorder) {
				gomock.InOrder(
					dynconfig.Get().Return(&config.DynconfigData{
						CDNs: []*config.CDN{{ID: 1}},
					}, nil).Times(1),
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
			name: "new cdn client failed because of dynconfig get error data",
			mock: func(dynconfig *configmocks.MockDynconfigInterfaceMockRecorder, hostManager *MockHostManagerMockRecorder) {
				dynconfig.Get().Return(nil, errors.New("foo")).Times(1)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "foo")
			},
		},
		{
			name: "new cdn client failed because of cdn list is empty",
			mock: func(dynconfig *configmocks.MockDynconfigInterfaceMockRecorder, hostManager *MockHostManagerMockRecorder) {
				gomock.InOrder(
					dynconfig.Get().Return(&config.DynconfigData{
						CDNs: []*config.CDN{},
					}, nil).Times(1),
				)
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "address list of cdn is empty")
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

			_, err := newCDNClient(dynconfig, hostManager)
			tc.expect(t, err)
		})
	}
}

func TestCDNClient_OnNotify(t *testing.T) {
	tests := []struct {
		name string
		data *config.DynconfigData
		mock func(dynconfig *configmocks.MockDynconfigInterfaceMockRecorder, hostManager *MockHostManagerMockRecorder)
	}{
		{
			name: "notify client without different cdns",
			data: &config.DynconfigData{
				CDNs: []*config.CDN{{
					ID:       1,
					Hostname: "foo",
					IP:       "0.0.0.0",
					Port:     8080,
				}},
			},
			mock: func(dynconfig *configmocks.MockDynconfigInterfaceMockRecorder, hostManager *MockHostManagerMockRecorder) {
				gomock.InOrder(
					dynconfig.Get().Return(&config.DynconfigData{
						CDNs: []*config.CDN{{
							ID:       1,
							Hostname: "foo",
							IP:       "0.0.0.0",
							Port:     8080,
						}},
					}, nil).Times(1),
					hostManager.Store(gomock.Any()).Return().Times(1),
					dynconfig.Register(gomock.Any()).Return().Times(1),
				)
			},
		},
		{
			name: "notify client with different cdns",
			data: &config.DynconfigData{
				CDNs: []*config.CDN{{
					ID:       1,
					Hostname: "foo",
					IP:       "0.0.0.0",
				}},
			},
			mock: func(dynconfig *configmocks.MockDynconfigInterfaceMockRecorder, hostManager *MockHostManagerMockRecorder) {
				mockHost := NewHost(mockRawHost)
				gomock.InOrder(
					dynconfig.Get().Return(&config.DynconfigData{
						CDNs: []*config.CDN{{
							ID:       1,
							Hostname: "foo",
							IP:       "127.0.0.1",
						}},
					}, nil).Times(1),
					hostManager.Store(gomock.Any()).Return().Times(1),
					dynconfig.Register(gomock.Any()).Return().Times(1),
					hostManager.Load(gomock.Any()).Return(mockHost, true).Times(1),
					hostManager.Delete(gomock.Eq("foo-0_CDN")).Return().Times(1),
					hostManager.Store(gomock.Any()).Return().Times(1),
				)
			},
		},
		{
			name: "notify client with different cdns and load host failed",
			data: &config.DynconfigData{
				CDNs: []*config.CDN{{
					ID:       1,
					Hostname: "foo",
					IP:       "0.0.0.0",
				}},
			},
			mock: func(dynconfig *configmocks.MockDynconfigInterfaceMockRecorder, hostManager *MockHostManagerMockRecorder) {
				gomock.InOrder(
					dynconfig.Get().Return(&config.DynconfigData{
						CDNs: []*config.CDN{{
							ID:       1,
							Hostname: "foo",
							IP:       "127.0.0.1",
						}},
					}, nil).Times(1),
					hostManager.Store(gomock.Any()).Return().Times(1),
					dynconfig.Register(gomock.Any()).Return().Times(1),
					hostManager.Load(gomock.Any()).Return(nil, false).Times(1),
					hostManager.Store(gomock.Any()).Return().Times(1),
				)
			},
		},
		{
			name: "cdn list is deep equal",
			data: &config.DynconfigData{
				CDNs: []*config.CDN{{
					ID: 1,
					IP: "127.0.0.1",
				}},
			},
			mock: func(dynconfig *configmocks.MockDynconfigInterfaceMockRecorder, hostManager *MockHostManagerMockRecorder) {
				gomock.InOrder(
					dynconfig.Get().Return(&config.DynconfigData{
						CDNs: []*config.CDN{{
							ID: 1,
							IP: "127.0.0.1",
						}},
					}, nil).Times(1),
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

			client, err := newCDNClient(dynconfig, hostManager)
			if err != nil {
				t.Fatal(err)
			}
			client.OnNotify(tc.data)
		})
	}
}

func TestCDNClient_cdnsToHosts(t *testing.T) {
	mockCDNClusterConfig, err := json.Marshal(&types.CDNClusterConfig{
		LoadLimit:   10,
		NetTopology: "foo",
	})
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		cdns   []*config.CDN
		expect func(t *testing.T, hosts map[string]*Host)
	}{
		{
			name: "cdns covert to hosts",
			cdns: []*config.CDN{
				{
					ID:           1,
					Hostname:     mockRawCDNHost.HostName,
					IP:           mockRawCDNHost.Ip,
					Port:         mockRawCDNHost.RpcPort,
					DownloadPort: mockRawCDNHost.DownPort,
					Location:     mockRawCDNHost.Location,
					IDC:          mockRawCDNHost.Idc,
					CDNCluster: &config.CDNCluster{
						Config: mockCDNClusterConfig,
					},
				},
			},
			expect: func(t *testing.T, hosts map[string]*Host) {
				assert := assert.New(t)
				assert.Equal(hosts[mockRawCDNHost.Uuid].ID, mockRawCDNHost.Uuid)
				assert.Equal(hosts[mockRawCDNHost.Uuid].IP, mockRawCDNHost.Ip)
				assert.Equal(hosts[mockRawCDNHost.Uuid].Hostname, mockRawCDNHost.HostName)
				assert.Equal(hosts[mockRawCDNHost.Uuid].Port, mockRawCDNHost.RpcPort)
				assert.Equal(hosts[mockRawCDNHost.Uuid].DownloadPort, mockRawCDNHost.DownPort)
				assert.Equal(hosts[mockRawCDNHost.Uuid].IDC, mockRawCDNHost.Idc)
				assert.Equal(hosts[mockRawCDNHost.Uuid].NetTopology, "foo")
				assert.Equal(hosts[mockRawCDNHost.Uuid].Location, mockRawCDNHost.Location)
				assert.Equal(hosts[mockRawCDNHost.Uuid].UploadLoadLimit.Load(), int32(10))
				assert.Empty(hosts[mockRawCDNHost.Uuid].Peers)
				assert.Equal(hosts[mockRawCDNHost.Uuid].IsCDN, true)
				assert.NotEqual(hosts[mockRawCDNHost.Uuid].CreateAt.Load(), 0)
				assert.NotEqual(hosts[mockRawCDNHost.Uuid].UpdateAt.Load(), 0)
				assert.NotNil(hosts[mockRawCDNHost.Uuid].Log)
			},
		},
		{
			name: "cdns covert to hosts without cluster config",
			cdns: []*config.CDN{
				{
					ID:           1,
					Hostname:     mockRawCDNHost.HostName,
					IP:           mockRawCDNHost.Ip,
					Port:         mockRawCDNHost.RpcPort,
					DownloadPort: mockRawCDNHost.DownPort,
					Location:     mockRawCDNHost.Location,
					IDC:          mockRawCDNHost.Idc,
				},
			},
			expect: func(t *testing.T, hosts map[string]*Host) {
				assert := assert.New(t)
				assert.Equal(hosts[mockRawCDNHost.Uuid].ID, mockRawCDNHost.Uuid)
				assert.Equal(hosts[mockRawCDNHost.Uuid].IP, mockRawCDNHost.Ip)
				assert.Equal(hosts[mockRawCDNHost.Uuid].Hostname, mockRawCDNHost.HostName)
				assert.Equal(hosts[mockRawCDNHost.Uuid].Port, mockRawCDNHost.RpcPort)
				assert.Equal(hosts[mockRawCDNHost.Uuid].DownloadPort, mockRawCDNHost.DownPort)
				assert.Equal(hosts[mockRawCDNHost.Uuid].IDC, mockRawCDNHost.Idc)
				assert.Equal(hosts[mockRawCDNHost.Uuid].NetTopology, "")
				assert.Equal(hosts[mockRawCDNHost.Uuid].Location, mockRawCDNHost.Location)
				assert.Equal(hosts[mockRawCDNHost.Uuid].UploadLoadLimit.Load(), int32(config.DefaultClientLoadLimit))
				assert.Empty(hosts[mockRawCDNHost.Uuid].Peers)
				assert.Equal(hosts[mockRawCDNHost.Uuid].IsCDN, true)
				assert.NotEqual(hosts[mockRawCDNHost.Uuid].CreateAt.Load(), 0)
				assert.NotEqual(hosts[mockRawCDNHost.Uuid].UpdateAt.Load(), 0)
				assert.NotNil(hosts[mockRawCDNHost.Uuid].Log)
			},
		},
		{
			name: "cdns is empty",
			cdns: []*config.CDN{},
			expect: func(t *testing.T, hosts map[string]*Host) {
				assert := assert.New(t)
				assert.Equal(len(hosts), 0)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, cdnsToHosts(tc.cdns))
		})
	}
}

func TestCDNClient_cdnsToNetAddrs(t *testing.T) {
	tests := []struct {
		name   string
		cdns   []*config.CDN
		expect func(t *testing.T, netAddrs []dfnet.NetAddr)
	}{
		{
			name: "cdns covert to netAddr",
			cdns: []*config.CDN{
				{
					ID:           1,
					Hostname:     mockRawCDNHost.HostName,
					IP:           mockRawCDNHost.Ip,
					Port:         mockRawCDNHost.RpcPort,
					DownloadPort: mockRawCDNHost.DownPort,
					Location:     mockRawCDNHost.Location,
					IDC:          mockRawCDNHost.Idc,
				},
			},
			expect: func(t *testing.T, netAddrs []dfnet.NetAddr) {
				assert := assert.New(t)
				assert.Equal(netAddrs[0].Type, dfnet.TCP)
				assert.Equal(netAddrs[0].Addr, fmt.Sprintf("%s:%d", mockRawCDNHost.Ip, mockRawCDNHost.RpcPort))
			},
		},
		{
			name: "cdns is empty",
			cdns: []*config.CDN{},
			expect: func(t *testing.T, netAddrs []dfnet.NetAddr) {
				assert := assert.New(t)
				assert.Equal(len(netAddrs), 0)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, cdnsToNetAddrs(tc.cdns))
		})
	}
}

func TestCDNClient_diffCDNs(t *testing.T) {
	tests := []struct {
		name   string
		cx     []*config.CDN
		cy     []*config.CDN
		expect func(t *testing.T, diff []*config.CDN)
	}{
		{
			name: "same cdn list",
			cx: []*config.CDN{
				{
					ID:       1,
					Hostname: "foo",
					IP:       "127.0.0.1",
					Port:     8080,
				},
			},
			cy: []*config.CDN{
				{
					ID:       1,
					Hostname: "foo",
					IP:       "127.0.0.1",
					Port:     8080,
				},
			},
			expect: func(t *testing.T, diff []*config.CDN) {
				assert := assert.New(t)
				assert.EqualValues(diff, []*config.CDN(nil))
			},
		},
		{
			name: "different hostname",
			cx: []*config.CDN{
				{
					ID:       1,
					Hostname: "bar",
					IP:       "127.0.0.1",
					Port:     8080,
				},
			},
			cy: []*config.CDN{
				{
					ID:       1,
					Hostname: "foo",
					IP:       "127.0.0.1",
					Port:     8080,
				},
			},
			expect: func(t *testing.T, diff []*config.CDN) {
				assert := assert.New(t)
				assert.EqualValues(diff, []*config.CDN{
					{
						ID:       1,
						Hostname: "bar",
						IP:       "127.0.0.1",
						Port:     8080,
					},
				})
			},
		},
		{
			name: "different port",
			cx: []*config.CDN{
				{
					ID:       1,
					Hostname: "foo",
					IP:       "127.0.0.1",
					Port:     8081,
				},
			},
			cy: []*config.CDN{
				{
					ID:       1,
					Hostname: "foo",
					IP:       "127.0.0.1",
					Port:     8080,
				},
			},
			expect: func(t *testing.T, diff []*config.CDN) {
				assert := assert.New(t)
				assert.EqualValues(diff, []*config.CDN{
					{
						ID:       1,
						Hostname: "foo",
						IP:       "127.0.0.1",
						Port:     8081,
					},
				})
			},
		},
		{
			name: "different ip",
			cx: []*config.CDN{
				{
					ID:       1,
					Hostname: "foo",
					IP:       "0.0.0.0",
					Port:     8080,
				},
			},
			cy: []*config.CDN{
				{
					ID:       1,
					Hostname: "foo",
					IP:       "127.0.0.1",
					Port:     8080,
				},
			},
			expect: func(t *testing.T, diff []*config.CDN) {
				assert := assert.New(t)
				assert.EqualValues(diff, []*config.CDN{
					{
						ID:       1,
						Hostname: "foo",
						IP:       "0.0.0.0",
						Port:     8080,
					},
				})
			},
		},
		{
			name: "remove y cdn",
			cx: []*config.CDN{
				{
					ID:       1,
					Hostname: "foo",
					IP:       "127.0.0.1",
					Port:     8080,
				},
				{
					ID:       2,
					Hostname: "bar",
					IP:       "127.0.0.1",
					Port:     8080,
				},
			},
			cy: []*config.CDN{
				{
					ID:       1,
					Hostname: "foo",
					IP:       "127.0.0.1",
					Port:     8080,
				},
			},
			expect: func(t *testing.T, diff []*config.CDN) {
				assert := assert.New(t)
				assert.EqualValues(diff, []*config.CDN{
					{
						ID:       2,
						Hostname: "bar",
						IP:       "127.0.0.1",
						Port:     8080,
					},
				})
			},
		},
		{
			name: "remove x cdn",
			cx: []*config.CDN{
				{
					ID:       1,
					Hostname: "foo",
					IP:       "127.0.0.1",
					Port:     8080,
				},
			},
			cy: []*config.CDN{
				{
					ID:       1,
					Hostname: "baz",
					IP:       "127.0.0.1",
					Port:     8080,
				},
				{
					ID:       2,
					Hostname: "bar",
					IP:       "127.0.0.1",
					Port:     8080,
				},
			},
			expect: func(t *testing.T, diff []*config.CDN) {
				assert := assert.New(t)
				assert.EqualValues(diff, []*config.CDN{
					{
						ID:       1,
						Hostname: "foo",
						IP:       "127.0.0.1",
						Port:     8080,
					},
				})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, diffCDNs(tc.cx, tc.cy))
		})
	}
}
