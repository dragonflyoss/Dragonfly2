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
	"testing"

	"github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/scheduler/config"
)

var (
	mockRawHost = &scheduler.PeerHost{
		Id:             idgen.HostID("hostname", 8003),
		Ip:             "127.0.0.1",
		RpcPort:        8003,
		DownPort:       8001,
		HostName:       "hostname",
		SecurityDomain: "security_domain",
		Location:       "location",
		Idc:            "idc",
		NetTopology:    "net_topology",
	}

	mockRawSeedHost = &scheduler.PeerHost{
		Id:             idgen.HostID("hostname_seed", 8003),
		Ip:             "127.0.0.1",
		RpcPort:        8003,
		DownPort:       8001,
		HostName:       "hostname_seed",
		SecurityDomain: "security_domain",
		Location:       "location",
		Idc:            "idc",
		NetTopology:    "net_topology",
	}
)

func TestHost_NewHost(t *testing.T) {
	tests := []struct {
		name    string
		rawHost *scheduler.PeerHost
		options []HostOption
		expect  func(t *testing.T, host *Host)
	}{
		{
			name:    "new host",
			rawHost: mockRawHost,
			expect: func(t *testing.T, host *Host) {
				assert := assert.New(t)
				assert.Equal(host.ID, mockRawHost.Id)
				assert.Equal(host.Type, HostTypeNormal)
				assert.Equal(host.IP, mockRawHost.Ip)
				assert.Equal(host.Port, mockRawHost.RpcPort)
				assert.Equal(host.DownloadPort, mockRawHost.DownPort)
				assert.Equal(host.Hostname, mockRawHost.HostName)
				assert.Equal(host.SecurityDomain, mockRawHost.SecurityDomain)
				assert.Equal(host.Location, mockRawHost.Location)
				assert.Equal(host.IDC, mockRawHost.Idc)
				assert.Equal(host.NetTopology, mockRawHost.NetTopology)
				assert.Equal(host.UploadLoadLimit.Load(), int32(config.DefaultClientLoadLimit))
				assert.Equal(host.PeerCount.Load(), int32(0))
				assert.NotEqual(host.CreateAt.Load(), 0)
				assert.NotEqual(host.UpdateAt.Load(), 0)
				assert.NotNil(host.Log)
			},
		},
		{
			name:    "new seed host",
			rawHost: mockRawSeedHost,
			options: []HostOption{WithHostType(HostTypeSuperSeed)},
			expect: func(t *testing.T, host *Host) {
				assert := assert.New(t)
				assert.Equal(host.ID, mockRawSeedHost.Id)
				assert.Equal(host.Type, HostTypeSuperSeed)
				assert.Equal(host.IP, mockRawSeedHost.Ip)
				assert.Equal(host.Port, mockRawSeedHost.RpcPort)
				assert.Equal(host.DownloadPort, mockRawSeedHost.DownPort)
				assert.Equal(host.Hostname, mockRawSeedHost.HostName)
				assert.Equal(host.SecurityDomain, mockRawSeedHost.SecurityDomain)
				assert.Equal(host.Location, mockRawSeedHost.Location)
				assert.Equal(host.IDC, mockRawSeedHost.Idc)
				assert.Equal(host.NetTopology, mockRawSeedHost.NetTopology)
				assert.Equal(host.UploadLoadLimit.Load(), int32(config.DefaultClientLoadLimit))
				assert.Equal(host.PeerCount.Load(), int32(0))
				assert.NotEqual(host.CreateAt.Load(), 0)
				assert.NotEqual(host.UpdateAt.Load(), 0)
				assert.NotNil(host.Log)
			},
		},
		{
			name:    "new host and set upload loadlimit",
			rawHost: mockRawHost,
			options: []HostOption{WithUploadLoadLimit(200)},
			expect: func(t *testing.T, host *Host) {
				assert := assert.New(t)
				assert.Equal(host.ID, mockRawHost.Id)
				assert.Equal(host.Type, HostTypeNormal)
				assert.Equal(host.IP, mockRawHost.Ip)
				assert.Equal(host.Port, mockRawHost.RpcPort)
				assert.Equal(host.DownloadPort, mockRawHost.DownPort)
				assert.Equal(host.Hostname, mockRawHost.HostName)
				assert.Equal(host.SecurityDomain, mockRawHost.SecurityDomain)
				assert.Equal(host.Location, mockRawHost.Location)
				assert.Equal(host.IDC, mockRawHost.Idc)
				assert.Equal(host.NetTopology, mockRawHost.NetTopology)
				assert.Equal(host.UploadLoadLimit.Load(), int32(200))
				assert.Equal(host.PeerCount.Load(), int32(0))
				assert.NotEqual(host.CreateAt.Load(), 0)
				assert.NotEqual(host.UpdateAt.Load(), 0)
				assert.NotNil(host.Log)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, NewHost(tc.rawHost, tc.options...))
		})
	}
}

func TestHost_LoadPeer(t *testing.T) {
	tests := []struct {
		name    string
		rawHost *scheduler.PeerHost
		peerID  string
		options []HostOption
		expect  func(t *testing.T, peer *Peer, ok bool)
	}{
		{
			name:    "load peer",
			rawHost: mockRawHost,
			peerID:  mockPeerID,
			expect: func(t *testing.T, peer *Peer, ok bool) {
				assert := assert.New(t)
				assert.Equal(ok, true)
				assert.Equal(peer.ID, mockPeerID)
			},
		},
		{
			name:    "peer does not exist",
			rawHost: mockRawHost,
			peerID:  idgen.PeerID("0.0.0.0"),
			expect: func(t *testing.T, peer *Peer, ok bool) {
				assert := assert.New(t)
				assert.Equal(ok, false)
			},
		},
		{
			name:    "load key is empty",
			rawHost: mockRawHost,
			peerID:  "",
			expect: func(t *testing.T, peer *Peer, ok bool) {
				assert := assert.New(t)
				assert.Equal(ok, false)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			host := NewHost(tc.rawHost, tc.options...)
			mockTask := NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, WithBackToSourceLimit(mockTaskBackToSourceLimit))
			mockPeer := NewPeer(mockPeerID, mockTask, host)

			host.StorePeer(mockPeer)
			peer, ok := host.LoadPeer(tc.peerID)
			tc.expect(t, peer, ok)
		})
	}
}

func TestHost_StorePeer(t *testing.T) {
	tests := []struct {
		name    string
		rawHost *scheduler.PeerHost
		peerID  string
		options []HostOption
		expect  func(t *testing.T, peer *Peer, ok bool)
	}{
		{
			name:    "store peer",
			rawHost: mockRawHost,
			peerID:  mockPeerID,
			expect: func(t *testing.T, peer *Peer, ok bool) {
				assert := assert.New(t)
				assert.Equal(ok, true)
				assert.Equal(peer.ID, mockPeerID)
			},
		},
		{
			name:    "store key is empty",
			rawHost: mockRawHost,
			peerID:  "",
			expect: func(t *testing.T, peer *Peer, ok bool) {
				assert := assert.New(t)
				assert.Equal(ok, true)
				assert.Equal(peer.ID, "")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			host := NewHost(tc.rawHost, tc.options...)
			mockTask := NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, WithBackToSourceLimit(mockTaskBackToSourceLimit))
			mockPeer := NewPeer(tc.peerID, mockTask, host)

			host.StorePeer(mockPeer)
			peer, ok := host.LoadPeer(tc.peerID)
			tc.expect(t, peer, ok)
		})
	}
}

func TestHost_LoadOrStorePeer(t *testing.T) {
	tests := []struct {
		name    string
		rawHost *scheduler.PeerHost
		peerID  string
		options []HostOption
		expect  func(t *testing.T, host *Host, mockPeer *Peer)
	}{
		{
			name:    "load peer exist",
			rawHost: mockRawHost,
			peerID:  mockPeerID,
			expect: func(t *testing.T, host *Host, mockPeer *Peer) {
				assert := assert.New(t)
				peer, ok := host.LoadOrStorePeer(mockPeer)

				assert.Equal(ok, true)
				assert.Equal(peer.ID, mockPeerID)
			},
		},
		{
			name:    "load peer does not exist",
			rawHost: mockRawHost,
			peerID:  mockPeerID,
			expect: func(t *testing.T, host *Host, mockPeer *Peer) {
				assert := assert.New(t)
				mockPeer.ID = idgen.PeerID("0.0.0.0")
				peer, ok := host.LoadOrStorePeer(mockPeer)

				assert.Equal(ok, false)
				assert.Equal(peer.ID, mockPeer.ID)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			host := NewHost(tc.rawHost, tc.options...)
			mockTask := NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, WithBackToSourceLimit(mockTaskBackToSourceLimit))
			mockPeer := NewPeer(mockPeerID, mockTask, host)

			host.StorePeer(mockPeer)
			tc.expect(t, host, mockPeer)
		})
	}
}

func TestHost_DeletePeer(t *testing.T) {
	tests := []struct {
		name    string
		rawHost *scheduler.PeerHost
		peerID  string
		options []HostOption
		expect  func(t *testing.T, host *Host)
	}{
		{
			name:    "delete peer",
			rawHost: mockRawHost,
			peerID:  mockPeerID,
			expect: func(t *testing.T, host *Host) {
				assert := assert.New(t)
				_, ok := host.LoadPeer(mockPeerID)
				assert.Equal(ok, false)
			},
		},
		{
			name:    "delete key is empty",
			rawHost: mockRawHost,
			peerID:  "",
			expect: func(t *testing.T, host *Host) {
				assert := assert.New(t)
				peer, ok := host.LoadPeer(mockPeerID)
				assert.Equal(ok, true)
				assert.Equal(peer.ID, mockPeerID)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			host := NewHost(tc.rawHost, tc.options...)
			mockTask := NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, WithBackToSourceLimit(mockTaskBackToSourceLimit))
			mockPeer := NewPeer(mockPeerID, mockTask, host)

			host.StorePeer(mockPeer)
			host.DeletePeer(tc.peerID)
			tc.expect(t, host)
		})
	}
}

func TestHost_LeavePeers(t *testing.T) {
	tests := []struct {
		name    string
		rawHost *scheduler.PeerHost
		options []HostOption
		expect  func(t *testing.T, host *Host, mockPeer *Peer)
	}{
		{
			name:    "leave peers",
			rawHost: mockRawHost,
			expect: func(t *testing.T, host *Host, mockPeer *Peer) {
				assert := assert.New(t)
				host.StorePeer(mockPeer)
				assert.Equal(host.PeerCount.Load(), int32(1))
				host.LeavePeers()
				host.Peers.Range(func(_, value any) bool {
					peer := value.(*Peer)
					assert.True(peer.FSM.Is(PeerStateLeave))
					return true
				})
			},
		},
		{
			name:    "peers is empty ",
			rawHost: mockRawHost,
			expect: func(t *testing.T, host *Host, mockPeer *Peer) {
				assert := assert.New(t)
				assert.Equal(host.PeerCount.Load(), int32(0))
				host.LeavePeers()
				host.Peers.Range(func(_, value any) bool {
					assert.Fail("host peers is not empty")
					return true
				})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			host := NewHost(tc.rawHost, tc.options...)
			mockTask := NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, WithBackToSourceLimit(mockTaskBackToSourceLimit))
			mockPeer := NewPeer(mockPeerID, mockTask, host)

			tc.expect(t, host, mockPeer)
		})
	}
}

func TestHost_FreeUploadLoad(t *testing.T) {
	tests := []struct {
		name    string
		rawHost *scheduler.PeerHost
		options []HostOption
		expect  func(t *testing.T, host *Host, mockPeer *Peer)
	}{
		{
			name:    "get free upload load",
			rawHost: mockRawHost,
			expect: func(t *testing.T, host *Host, mockPeer *Peer) {
				assert := assert.New(t)
				mockPeer.StoreParent(mockPeer)
				assert.Equal(host.FreeUploadLoad(), int32(config.DefaultClientLoadLimit-1))
				mockPeer.StoreParent(mockPeer)
				assert.Equal(host.FreeUploadLoad(), int32(config.DefaultClientLoadLimit-1))
				mockPeer.DeleteParent()
				assert.Equal(host.FreeUploadLoad(), int32(config.DefaultClientLoadLimit))
			},
		},
		{
			name:    "upload peer does not exist",
			rawHost: mockRawHost,
			expect: func(t *testing.T, host *Host, mockPeer *Peer) {
				assert := assert.New(t)
				assert.Equal(host.FreeUploadLoad(), int32(config.DefaultClientLoadLimit))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			host := NewHost(tc.rawHost, tc.options...)
			mockTask := NewTask(mockTaskID, mockTaskURL, base.TaskType_Normal, mockTaskURLMeta, WithBackToSourceLimit(mockTaskBackToSourceLimit))
			mockPeer := NewPeer(mockPeerID, mockTask, host)

			tc.expect(t, host, mockPeer)
		})
	}
}
