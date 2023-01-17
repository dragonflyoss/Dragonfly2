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

	commonv1 "d7y.io/api/pkg/apis/common/v1"
	schedulerv1 "d7y.io/api/pkg/apis/scheduler/v1"

	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/types"
	"d7y.io/dragonfly/v2/scheduler/config"
)

var (
	mockRawHost = &schedulerv1.AnnounceHostRequest{
		Id:           idgen.HostID("hostname", 8003),
		Type:         types.HostTypeNormalName,
		Ip:           "127.0.0.1",
		Port:         8003,
		DownloadPort: 8001,
		Hostname:     "hostname",
		Network: &schedulerv1.Network{
			SecurityDomain: "security_domain",
			Location:       "location",
			Idc:            "idc",
		},
	}

	mockRawSeedHost = &schedulerv1.AnnounceHostRequest{
		Id:           idgen.HostID("hostname_seed", 8003),
		Type:         types.HostTypeSuperSeedName,
		Ip:           "127.0.0.1",
		Port:         8003,
		DownloadPort: 8001,
		Hostname:     "hostname_seed",
		Network: &schedulerv1.Network{
			SecurityDomain: "security_domain",
			Location:       "location",
			Idc:            "idc",
		},
	}
)

func TestHost_NewHost(t *testing.T) {
	tests := []struct {
		name    string
		rawHost *schedulerv1.AnnounceHostRequest
		options []HostOption
		expect  func(t *testing.T, host *Host)
	}{
		{
			name:    "new host",
			rawHost: mockRawHost,
			expect: func(t *testing.T, host *Host) {
				assert := assert.New(t)
				assert.Equal(host.ID, mockRawHost.Id)
				assert.Equal(host.Type, types.HostTypeNormal)
				assert.Equal(host.IP, mockRawHost.Ip)
				assert.Equal(host.Port, mockRawHost.Port)
				assert.Equal(host.DownloadPort, mockRawHost.DownloadPort)
				assert.Equal(host.Hostname, mockRawHost.Hostname)
				assert.Equal(host.Network.SecurityDomain, mockRawHost.Network.SecurityDomain)
				assert.Equal(host.Network.Location, mockRawHost.Network.Location)
				assert.Equal(host.Network.Idc, mockRawHost.Network.Idc)
				assert.Equal(host.ConcurrentUploadLimit.Load(), int32(config.DefaultPeerConcurrentUploadLimit))
				assert.Equal(host.PeerCount.Load(), int32(0))
				assert.NotEqual(host.CreatedAt.Load(), 0)
				assert.NotEqual(host.UpdatedAt.Load(), 0)
				assert.NotNil(host.Log)
			},
		},
		{
			name:    "new seed host",
			rawHost: mockRawSeedHost,
			expect: func(t *testing.T, host *Host) {
				assert := assert.New(t)
				assert.Equal(host.ID, mockRawSeedHost.Id)
				assert.Equal(host.Type, types.HostTypeSuperSeed)
				assert.Equal(host.IP, mockRawSeedHost.Ip)
				assert.Equal(host.Port, mockRawSeedHost.Port)
				assert.Equal(host.DownloadPort, mockRawSeedHost.DownloadPort)
				assert.Equal(host.Hostname, mockRawSeedHost.Hostname)
				assert.Equal(host.Network.SecurityDomain, mockRawSeedHost.Network.SecurityDomain)
				assert.Equal(host.Network.Location, mockRawSeedHost.Network.Location)
				assert.Equal(host.Network.Idc, mockRawSeedHost.Network.Idc)
				assert.Equal(host.ConcurrentUploadLimit.Load(), int32(config.DefaultPeerConcurrentUploadLimit))
				assert.Equal(host.PeerCount.Load(), int32(0))
				assert.NotEqual(host.CreatedAt.Load(), 0)
				assert.NotEqual(host.UpdatedAt.Load(), 0)
				assert.NotNil(host.Log)
			},
		},
		{
			name:    "new host and set upload loadlimit",
			rawHost: mockRawHost,
			options: []HostOption{WithConcurrentUploadLimit(200)},
			expect: func(t *testing.T, host *Host) {
				assert := assert.New(t)
				assert.Equal(host.ID, mockRawHost.Id)
				assert.Equal(host.Type, types.HostTypeNormal)
				assert.Equal(host.IP, mockRawHost.Ip)
				assert.Equal(host.Port, mockRawHost.Port)
				assert.Equal(host.DownloadPort, mockRawHost.DownloadPort)
				assert.Equal(host.Hostname, mockRawHost.Hostname)
				assert.Equal(host.Network.SecurityDomain, mockRawHost.Network.SecurityDomain)
				assert.Equal(host.Network.Location, mockRawHost.Network.Location)
				assert.Equal(host.Network.Idc, mockRawHost.Network.Idc)
				assert.Equal(host.ConcurrentUploadLimit.Load(), int32(200))
				assert.Equal(host.PeerCount.Load(), int32(0))
				assert.NotEqual(host.CreatedAt.Load(), 0)
				assert.NotEqual(host.UpdatedAt.Load(), 0)
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
		rawHost *schedulerv1.AnnounceHostRequest
		peerID  string
		options []HostOption
		expect  func(t *testing.T, peer *Peer, loaded bool)
	}{
		{
			name:    "load peer",
			rawHost: mockRawHost,
			peerID:  mockPeerID,
			expect: func(t *testing.T, peer *Peer, loaded bool) {
				assert := assert.New(t)
				assert.Equal(loaded, true)
				assert.Equal(peer.ID, mockPeerID)
			},
		},
		{
			name:    "peer does not exist",
			rawHost: mockRawHost,
			peerID:  idgen.PeerID("0.0.0.0"),
			expect: func(t *testing.T, peer *Peer, loaded bool) {
				assert := assert.New(t)
				assert.Equal(loaded, false)
			},
		},
		{
			name:    "load key is empty",
			rawHost: mockRawHost,
			peerID:  "",
			expect: func(t *testing.T, peer *Peer, loaded bool) {
				assert := assert.New(t)
				assert.Equal(loaded, false)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			host := NewHost(tc.rawHost, tc.options...)
			mockTask := NewTask(mockTaskID, mockTaskURL, commonv1.TaskType_Normal, mockTaskURLMeta, WithBackToSourceLimit(mockTaskBackToSourceLimit))
			mockPeer := NewPeer(mockPeerID, mockTask, host)

			host.StorePeer(mockPeer)
			peer, loaded := host.LoadPeer(tc.peerID)
			tc.expect(t, peer, loaded)
		})
	}
}

func TestHost_StorePeer(t *testing.T) {
	tests := []struct {
		name    string
		rawHost *schedulerv1.AnnounceHostRequest
		peerID  string
		options []HostOption
		expect  func(t *testing.T, peer *Peer, loaded bool)
	}{
		{
			name:    "store peer",
			rawHost: mockRawHost,
			peerID:  mockPeerID,
			expect: func(t *testing.T, peer *Peer, loaded bool) {
				assert := assert.New(t)
				assert.Equal(loaded, true)
				assert.Equal(peer.ID, mockPeerID)
			},
		},
		{
			name:    "store key is empty",
			rawHost: mockRawHost,
			peerID:  "",
			expect: func(t *testing.T, peer *Peer, loaded bool) {
				assert := assert.New(t)
				assert.Equal(loaded, true)
				assert.Equal(peer.ID, "")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			host := NewHost(tc.rawHost, tc.options...)
			mockTask := NewTask(mockTaskID, mockTaskURL, commonv1.TaskType_Normal, mockTaskURLMeta, WithBackToSourceLimit(mockTaskBackToSourceLimit))
			mockPeer := NewPeer(tc.peerID, mockTask, host)

			host.StorePeer(mockPeer)
			peer, loaded := host.LoadPeer(tc.peerID)
			tc.expect(t, peer, loaded)
		})
	}
}

func TestHost_DeletePeer(t *testing.T) {
	tests := []struct {
		name    string
		rawHost *schedulerv1.AnnounceHostRequest
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
				_, loaded := host.LoadPeer(mockPeerID)
				assert.Equal(loaded, false)
			},
		},
		{
			name:    "delete key is empty",
			rawHost: mockRawHost,
			peerID:  "",
			expect: func(t *testing.T, host *Host) {
				assert := assert.New(t)
				peer, loaded := host.LoadPeer(mockPeerID)
				assert.Equal(loaded, true)
				assert.Equal(peer.ID, mockPeerID)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			host := NewHost(tc.rawHost, tc.options...)
			mockTask := NewTask(mockTaskID, mockTaskURL, commonv1.TaskType_Normal, mockTaskURLMeta, WithBackToSourceLimit(mockTaskBackToSourceLimit))
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
		rawHost *schedulerv1.AnnounceHostRequest
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
			mockTask := NewTask(mockTaskID, mockTaskURL, commonv1.TaskType_Normal, mockTaskURLMeta, WithBackToSourceLimit(mockTaskBackToSourceLimit))
			mockPeer := NewPeer(mockPeerID, mockTask, host)

			tc.expect(t, host, mockPeer)
		})
	}
}

func TestHost_FreeUploadCount(t *testing.T) {
	tests := []struct {
		name    string
		rawHost *schedulerv1.AnnounceHostRequest
		options []HostOption
		expect  func(t *testing.T, host *Host, mockTask *Task, mockPeer *Peer)
	}{
		{
			name:    "get free upload load",
			rawHost: mockRawHost,
			expect: func(t *testing.T, host *Host, mockTask *Task, mockPeer *Peer) {
				assert := assert.New(t)
				mockSeedPeer := NewPeer(mockSeedPeerID, mockTask, host)
				mockPeer.Task.StorePeer(mockSeedPeer)
				mockPeer.Task.StorePeer(mockPeer)
				err := mockPeer.Task.AddPeerEdge(mockSeedPeer, mockPeer)
				assert.NoError(err)
				assert.Equal(host.FreeUploadCount(), int32(config.DefaultPeerConcurrentUploadLimit-1))
				err = mockTask.DeletePeerInEdges(mockPeer.ID)
				assert.NoError(err)
				assert.Equal(host.FreeUploadCount(), int32(config.DefaultPeerConcurrentUploadLimit))
				err = mockPeer.Task.AddPeerEdge(mockSeedPeer, mockPeer)
				assert.NoError(err)
				assert.Equal(host.FreeUploadCount(), int32(config.DefaultPeerConcurrentUploadLimit-1))
				err = mockTask.DeletePeerOutEdges(mockSeedPeer.ID)
				assert.NoError(err)
				assert.Equal(host.FreeUploadCount(), int32(config.DefaultPeerConcurrentUploadLimit))
			},
		},
		{
			name:    "upload peer does not exist",
			rawHost: mockRawHost,
			expect: func(t *testing.T, host *Host, mockTask *Task, mockPeer *Peer) {
				assert := assert.New(t)
				assert.Equal(host.FreeUploadCount(), int32(config.DefaultPeerConcurrentUploadLimit))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			host := NewHost(tc.rawHost, tc.options...)
			mockTask := NewTask(mockTaskID, mockTaskURL, commonv1.TaskType_Normal, mockTaskURLMeta, WithBackToSourceLimit(mockTaskBackToSourceLimit))
			mockPeer := NewPeer(mockPeerID, mockTask, host)

			tc.expect(t, host, mockTask, mockPeer)
		})
	}
}
