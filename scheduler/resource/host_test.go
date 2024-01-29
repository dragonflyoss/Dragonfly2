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
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"

	commonv2 "d7y.io/api/v2/pkg/apis/common/v2"

	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/types"
	"d7y.io/dragonfly/v2/scheduler/config"
)

var (
	mockRawHost = Host{
		ID:              mockHostID,
		Type:            types.HostTypeNormal,
		Hostname:        "foo",
		IP:              "127.0.0.1",
		Port:            8003,
		DownloadPort:    8001,
		OS:              "darwin",
		Platform:        "darwin",
		PlatformFamily:  "Standalone Workstation",
		PlatformVersion: "11.1",
		KernelVersion:   "20.2.0",
		CPU:             mockCPU,
		Memory:          mockMemory,
		Network:         mockNetwork,
		Disk:            mockDisk,
		Build:           mockBuild,
		CreatedAt:       atomic.NewTime(time.Now()),
		UpdatedAt:       atomic.NewTime(time.Now()),
	}

	mockRawSeedHost = Host{
		ID:              mockSeedHostID,
		Type:            types.HostTypeSuperSeed,
		Hostname:        "bar",
		IP:              "127.0.0.1",
		Port:            8003,
		DownloadPort:    8001,
		OS:              "darwin",
		Platform:        "darwin",
		PlatformFamily:  "Standalone Workstation",
		PlatformVersion: "11.1",
		KernelVersion:   "20.2.0",
		CPU:             mockCPU,
		Memory:          mockMemory,
		Network:         mockNetwork,
		Disk:            mockDisk,
		Build:           mockBuild,
		CreatedAt:       atomic.NewTime(time.Now()),
		UpdatedAt:       atomic.NewTime(time.Now()),
	}

	mockCPU = CPU{
		LogicalCount:   4,
		PhysicalCount:  2,
		Percent:        1,
		ProcessPercent: 0.5,
		Times: CPUTimes{
			User:      240662.2,
			System:    317950.1,
			Idle:      3393691.3,
			Nice:      0,
			Iowait:    0,
			Irq:       0,
			Softirq:   0,
			Steal:     0,
			Guest:     0,
			GuestNice: 0,
		},
	}

	mockMemory = Memory{
		Total:              17179869184,
		Available:          5962813440,
		Used:               11217055744,
		UsedPercent:        65.291858,
		ProcessUsedPercent: 41.525125,
		Free:               2749598908,
	}

	mockNetwork = Network{
		TCPConnectionCount:       10,
		UploadTCPConnectionCount: 1,
		Location:                 mockHostLocation,
		IDC:                      mockHostIDC,
	}

	mockDisk = Disk{
		Total:             499963174912,
		Free:              37226479616,
		Used:              423809622016,
		UsedPercent:       91.92547406065952,
		InodesTotal:       4882452880,
		InodesUsed:        7835772,
		InodesFree:        4874617108,
		InodesUsedPercent: 0.1604884305611568,
	}

	mockBuild = Build{
		GitVersion: "v1.0.0",
		GitCommit:  "221176b117c6d59366d68f2b34d38be50c935883",
		GoVersion:  "1.18",
		Platform:   "darwin",
	}

	mockHostID       = idgen.HostIDV2("127.0.0.1", "foo")
	mockSeedHostID   = idgen.HostIDV2("127.0.0.1", "bar")
	mockHostLocation = "baz"
	mockHostIDC      = "bas"
)

func TestHost_NewHost(t *testing.T) {
	tests := []struct {
		name    string
		rawHost Host
		options []HostOption
		expect  func(t *testing.T, host *Host)
	}{
		{
			name:    "new host",
			rawHost: mockRawHost,
			expect: func(t *testing.T, host *Host) {
				assert := assert.New(t)
				assert.Equal(host.ID, mockRawHost.ID)
				assert.Equal(host.Type, types.HostTypeNormal)
				assert.Equal(host.Hostname, mockRawHost.Hostname)
				assert.Equal(host.IP, mockRawHost.IP)
				assert.Equal(host.Port, mockRawHost.Port)
				assert.Equal(host.DownloadPort, mockRawHost.DownloadPort)
				assert.Equal(host.ObjectStoragePort, int32(0))
				assert.Equal(host.SchedulerClusterID, uint64(0))
				assert.Equal(host.ConcurrentUploadLimit.Load(), int32(config.DefaultPeerConcurrentUploadLimit))
				assert.Equal(host.ConcurrentUploadCount.Load(), int32(0))
				assert.Equal(host.UploadCount.Load(), int64(0))
				assert.Equal(host.UploadFailedCount.Load(), int64(0))
				assert.NotNil(host.Peers)
				assert.Equal(host.PeerCount.Load(), int32(0))
				assert.NotEmpty(host.CreatedAt.Load())
				assert.NotEmpty(host.UpdatedAt.Load())
				assert.NotNil(host.Log)
			},
		},
		{
			name:    "new seed host",
			rawHost: mockRawSeedHost,
			expect: func(t *testing.T, host *Host) {
				assert := assert.New(t)
				assert.Equal(host.ID, mockRawSeedHost.ID)
				assert.Equal(host.Type, mockRawSeedHost.Type)
				assert.Equal(host.Hostname, mockRawSeedHost.Hostname)
				assert.Equal(host.IP, mockRawSeedHost.IP)
				assert.Equal(host.Port, mockRawSeedHost.Port)
				assert.Equal(host.DownloadPort, mockRawSeedHost.DownloadPort)
				assert.Equal(host.ObjectStoragePort, int32(0))
				assert.Equal(host.SchedulerClusterID, uint64(0))
				assert.Equal(host.ConcurrentUploadLimit.Load(), int32(config.DefaultSeedPeerConcurrentUploadLimit))
				assert.Equal(host.ConcurrentUploadCount.Load(), int32(0))
				assert.Equal(host.UploadCount.Load(), int64(0))
				assert.Equal(host.UploadFailedCount.Load(), int64(0))
				assert.NotNil(host.Peers)
				assert.Equal(host.PeerCount.Load(), int32(0))
				assert.NotEmpty(host.CreatedAt.Load())
				assert.NotEmpty(host.UpdatedAt.Load())
				assert.NotNil(host.Log)
			},
		},
		{
			name:    "new host and set scheduler cluster id",
			rawHost: mockRawHost,
			options: []HostOption{WithSchedulerClusterID(1)},
			expect: func(t *testing.T, host *Host) {
				assert := assert.New(t)
				assert.Equal(host.ID, mockRawHost.ID)
				assert.Equal(host.Type, types.HostTypeNormal)
				assert.Equal(host.Hostname, mockRawHost.Hostname)
				assert.Equal(host.IP, mockRawHost.IP)
				assert.Equal(host.Port, mockRawHost.Port)
				assert.Equal(host.DownloadPort, mockRawHost.DownloadPort)
				assert.Equal(host.SchedulerClusterID, uint64(1))
				assert.Equal(host.ConcurrentUploadLimit.Load(), int32(config.DefaultPeerConcurrentUploadLimit))
				assert.Equal(host.ConcurrentUploadCount.Load(), int32(0))
				assert.Equal(host.UploadCount.Load(), int64(0))
				assert.Equal(host.UploadFailedCount.Load(), int64(0))
				assert.NotNil(host.Peers)
				assert.Equal(host.PeerCount.Load(), int32(0))
				assert.NotEmpty(host.CreatedAt.Load())
				assert.NotEmpty(host.UpdatedAt.Load())
				assert.NotNil(host.Log)
			},
		},
		{
			name:    "new host and set object storage port",
			rawHost: mockRawHost,
			options: []HostOption{WithObjectStoragePort(1)},
			expect: func(t *testing.T, host *Host) {
				assert := assert.New(t)
				assert.Equal(host.ID, mockRawHost.ID)
				assert.Equal(host.Type, types.HostTypeNormal)
				assert.Equal(host.Hostname, mockRawHost.Hostname)
				assert.Equal(host.IP, mockRawHost.IP)
				assert.Equal(host.Port, mockRawHost.Port)
				assert.Equal(host.DownloadPort, mockRawHost.DownloadPort)
				assert.Equal(host.ObjectStoragePort, int32(1))
				assert.Equal(host.SchedulerClusterID, uint64(0))
				assert.Equal(host.ConcurrentUploadLimit.Load(), int32(config.DefaultPeerConcurrentUploadLimit))
				assert.Equal(host.ConcurrentUploadCount.Load(), int32(0))
				assert.Equal(host.UploadCount.Load(), int64(0))
				assert.Equal(host.UploadFailedCount.Load(), int64(0))
				assert.NotNil(host.Peers)
				assert.Equal(host.PeerCount.Load(), int32(0))
				assert.NotEmpty(host.CreatedAt.Load())
				assert.NotEmpty(host.UpdatedAt.Load())
				assert.NotNil(host.Log)
			},
		},
		{
			name:    "new host and set upload loadlimit",
			rawHost: mockRawHost,
			options: []HostOption{WithConcurrentUploadLimit(200)},
			expect: func(t *testing.T, host *Host) {
				assert := assert.New(t)
				assert.Equal(host.ID, mockRawHost.ID)
				assert.Equal(host.Type, types.HostTypeNormal)
				assert.Equal(host.Hostname, mockRawHost.Hostname)
				assert.Equal(host.IP, mockRawHost.IP)
				assert.Equal(host.Port, mockRawHost.Port)
				assert.Equal(host.DownloadPort, mockRawHost.DownloadPort)
				assert.Equal(host.ObjectStoragePort, int32(0))
				assert.Equal(host.SchedulerClusterID, uint64(0))
				assert.Equal(host.ConcurrentUploadLimit.Load(), int32(200))
				assert.Equal(host.ConcurrentUploadCount.Load(), int32(0))
				assert.Equal(host.UploadCount.Load(), int64(0))
				assert.Equal(host.UploadFailedCount.Load(), int64(0))
				assert.NotNil(host.Peers)
				assert.Equal(host.PeerCount.Load(), int32(0))
				assert.NotEmpty(host.CreatedAt.Load())
				assert.NotEmpty(host.UpdatedAt.Load())
				assert.NotNil(host.Log)
			},
		},
		{
			name:    "new host and set os",
			rawHost: mockRawHost,
			options: []HostOption{WithOS("linux")},
			expect: func(t *testing.T, host *Host) {
				assert := assert.New(t)
				assert.Equal(host.ID, mockRawHost.ID)
				assert.Equal(host.Type, types.HostTypeNormal)
				assert.Equal(host.Hostname, mockRawHost.Hostname)
				assert.Equal(host.IP, mockRawHost.IP)
				assert.Equal(host.Port, mockRawHost.Port)
				assert.Equal(host.DownloadPort, mockRawHost.DownloadPort)
				assert.Equal(host.ObjectStoragePort, int32(0))
				assert.Equal(host.OS, "linux")
				assert.Equal(host.SchedulerClusterID, uint64(0))
				assert.Equal(host.ConcurrentUploadLimit.Load(), int32(config.DefaultPeerConcurrentUploadLimit))
				assert.Equal(host.ConcurrentUploadCount.Load(), int32(0))
				assert.Equal(host.UploadCount.Load(), int64(0))
				assert.Equal(host.UploadFailedCount.Load(), int64(0))
				assert.NotNil(host.Peers)
				assert.Equal(host.PeerCount.Load(), int32(0))
				assert.NotEmpty(host.CreatedAt.Load())
				assert.NotEmpty(host.UpdatedAt.Load())
				assert.NotNil(host.Log)
			},
		},
		{
			name:    "new host and set platform",
			rawHost: mockRawHost,
			options: []HostOption{WithPlatform("ubuntu")},
			expect: func(t *testing.T, host *Host) {
				assert := assert.New(t)
				assert.Equal(host.ID, mockRawHost.ID)
				assert.Equal(host.Type, types.HostTypeNormal)
				assert.Equal(host.Hostname, mockRawHost.Hostname)
				assert.Equal(host.IP, mockRawHost.IP)
				assert.Equal(host.Port, mockRawHost.Port)
				assert.Equal(host.DownloadPort, mockRawHost.DownloadPort)
				assert.Equal(host.ObjectStoragePort, int32(0))
				assert.Equal(host.Platform, "ubuntu")
				assert.Equal(host.SchedulerClusterID, uint64(0))
				assert.Equal(host.ConcurrentUploadLimit.Load(), int32(config.DefaultPeerConcurrentUploadLimit))
				assert.Equal(host.ConcurrentUploadCount.Load(), int32(0))
				assert.Equal(host.UploadCount.Load(), int64(0))
				assert.Equal(host.UploadFailedCount.Load(), int64(0))
				assert.NotNil(host.Peers)
				assert.Equal(host.PeerCount.Load(), int32(0))
				assert.NotEmpty(host.CreatedAt.Load())
				assert.NotEmpty(host.UpdatedAt.Load())
				assert.NotNil(host.Log)
			},
		},
		{
			name:    "new host and set platform family",
			rawHost: mockRawHost,
			options: []HostOption{WithPlatformFamily("debian")},
			expect: func(t *testing.T, host *Host) {
				assert := assert.New(t)
				assert.Equal(host.ID, mockRawHost.ID)
				assert.Equal(host.Type, types.HostTypeNormal)
				assert.Equal(host.Hostname, mockRawHost.Hostname)
				assert.Equal(host.IP, mockRawHost.IP)
				assert.Equal(host.Port, mockRawHost.Port)
				assert.Equal(host.DownloadPort, mockRawHost.DownloadPort)
				assert.Equal(host.ObjectStoragePort, int32(0))
				assert.Equal(host.PlatformFamily, "debian")
				assert.Equal(host.SchedulerClusterID, uint64(0))
				assert.Equal(host.ConcurrentUploadLimit.Load(), int32(config.DefaultPeerConcurrentUploadLimit))
				assert.Equal(host.ConcurrentUploadCount.Load(), int32(0))
				assert.Equal(host.UploadCount.Load(), int64(0))
				assert.Equal(host.UploadFailedCount.Load(), int64(0))
				assert.NotNil(host.Peers)
				assert.Equal(host.PeerCount.Load(), int32(0))
				assert.NotEmpty(host.CreatedAt.Load())
				assert.NotEmpty(host.UpdatedAt.Load())
				assert.NotNil(host.Log)
			},
		},
		{
			name:    "new host and set platform version",
			rawHost: mockRawHost,
			options: []HostOption{WithPlatformVersion("22.04")},
			expect: func(t *testing.T, host *Host) {
				assert := assert.New(t)
				assert.Equal(host.ID, mockRawHost.ID)
				assert.Equal(host.Type, types.HostTypeNormal)
				assert.Equal(host.Hostname, mockRawHost.Hostname)
				assert.Equal(host.IP, mockRawHost.IP)
				assert.Equal(host.Port, mockRawHost.Port)
				assert.Equal(host.DownloadPort, mockRawHost.DownloadPort)
				assert.Equal(host.ObjectStoragePort, int32(0))
				assert.Equal(host.PlatformVersion, "22.04")
				assert.Equal(host.ConcurrentUploadLimit.Load(), int32(config.DefaultPeerConcurrentUploadLimit))
				assert.Equal(host.ConcurrentUploadCount.Load(), int32(0))
				assert.Equal(host.UploadCount.Load(), int64(0))
				assert.Equal(host.UploadFailedCount.Load(), int64(0))
				assert.NotNil(host.Peers)
				assert.Equal(host.PeerCount.Load(), int32(0))
				assert.NotEmpty(host.CreatedAt.Load())
				assert.NotEmpty(host.UpdatedAt.Load())
				assert.NotNil(host.Log)
			},
		},
		{
			name:    "new host and set kernel version",
			rawHost: mockRawHost,
			options: []HostOption{WithKernelVersion("5.15.0-27-generic")},
			expect: func(t *testing.T, host *Host) {
				assert := assert.New(t)
				assert.Equal(host.ID, mockRawHost.ID)
				assert.Equal(host.Type, types.HostTypeNormal)
				assert.Equal(host.Hostname, mockRawHost.Hostname)
				assert.Equal(host.IP, mockRawHost.IP)
				assert.Equal(host.Port, mockRawHost.Port)
				assert.Equal(host.DownloadPort, mockRawHost.DownloadPort)
				assert.Equal(host.ObjectStoragePort, int32(0))
				assert.Equal(host.KernelVersion, "5.15.0-27-generic")
				assert.Equal(host.SchedulerClusterID, uint64(0))
				assert.Equal(host.ConcurrentUploadLimit.Load(), int32(config.DefaultPeerConcurrentUploadLimit))
				assert.Equal(host.ConcurrentUploadCount.Load(), int32(0))
				assert.Equal(host.UploadCount.Load(), int64(0))
				assert.Equal(host.UploadFailedCount.Load(), int64(0))
				assert.NotNil(host.Peers)
				assert.Equal(host.PeerCount.Load(), int32(0))
				assert.NotEmpty(host.CreatedAt.Load())
				assert.NotEmpty(host.UpdatedAt.Load())
				assert.NotNil(host.Log)
			},
		},
		{
			name:    "new host and set cpu",
			rawHost: mockRawHost,
			options: []HostOption{WithCPU(mockCPU)},
			expect: func(t *testing.T, host *Host) {
				assert := assert.New(t)
				assert.Equal(host.ID, mockRawHost.ID)
				assert.Equal(host.Type, types.HostTypeNormal)
				assert.Equal(host.Hostname, mockRawHost.Hostname)
				assert.Equal(host.IP, mockRawHost.IP)
				assert.Equal(host.Port, mockRawHost.Port)
				assert.Equal(host.DownloadPort, mockRawHost.DownloadPort)
				assert.Equal(host.ObjectStoragePort, int32(0))
				assert.EqualValues(host.CPU, mockCPU)
				assert.Equal(host.SchedulerClusterID, uint64(0))
				assert.Equal(host.ConcurrentUploadLimit.Load(), int32(config.DefaultPeerConcurrentUploadLimit))
				assert.Equal(host.ConcurrentUploadCount.Load(), int32(0))
				assert.Equal(host.UploadCount.Load(), int64(0))
				assert.Equal(host.UploadFailedCount.Load(), int64(0))
				assert.NotNil(host.Peers)
				assert.Equal(host.PeerCount.Load(), int32(0))
				assert.NotEmpty(host.CreatedAt.Load())
				assert.NotEmpty(host.UpdatedAt.Load())
				assert.NotNil(host.Log)
			},
		},
		{
			name:    "new host and set memory",
			rawHost: mockRawHost,
			options: []HostOption{WithMemory(mockMemory)},
			expect: func(t *testing.T, host *Host) {
				assert := assert.New(t)
				assert.Equal(host.ID, mockRawHost.ID)
				assert.Equal(host.Type, types.HostTypeNormal)
				assert.Equal(host.Hostname, mockRawHost.Hostname)
				assert.Equal(host.IP, mockRawHost.IP)
				assert.Equal(host.Port, mockRawHost.Port)
				assert.Equal(host.DownloadPort, mockRawHost.DownloadPort)
				assert.Equal(host.ObjectStoragePort, int32(0))
				assert.EqualValues(host.Memory, mockMemory)
				assert.Equal(host.SchedulerClusterID, uint64(0))
				assert.Equal(host.ConcurrentUploadLimit.Load(), int32(config.DefaultPeerConcurrentUploadLimit))
				assert.Equal(host.ConcurrentUploadCount.Load(), int32(0))
				assert.Equal(host.UploadCount.Load(), int64(0))
				assert.Equal(host.UploadFailedCount.Load(), int64(0))
				assert.NotNil(host.Peers)
				assert.Equal(host.PeerCount.Load(), int32(0))
				assert.NotEmpty(host.CreatedAt.Load())
				assert.NotEmpty(host.UpdatedAt.Load())
				assert.NotNil(host.Log)
			},
		},
		{
			name:    "new host and set network",
			rawHost: mockRawHost,
			options: []HostOption{WithNetwork(mockNetwork)},
			expect: func(t *testing.T, host *Host) {
				assert := assert.New(t)
				assert.Equal(host.ID, mockRawHost.ID)
				assert.Equal(host.Type, types.HostTypeNormal)
				assert.Equal(host.Hostname, mockRawHost.Hostname)
				assert.Equal(host.IP, mockRawHost.IP)
				assert.Equal(host.Port, mockRawHost.Port)
				assert.Equal(host.DownloadPort, mockRawHost.DownloadPort)
				assert.Equal(host.ObjectStoragePort, int32(0))
				assert.EqualValues(host.Network, mockNetwork)
				assert.Equal(host.SchedulerClusterID, uint64(0))
				assert.Equal(host.ConcurrentUploadLimit.Load(), int32(config.DefaultPeerConcurrentUploadLimit))
				assert.Equal(host.ConcurrentUploadCount.Load(), int32(0))
				assert.Equal(host.UploadCount.Load(), int64(0))
				assert.Equal(host.UploadFailedCount.Load(), int64(0))
				assert.NotNil(host.Peers)
				assert.Equal(host.PeerCount.Load(), int32(0))
				assert.NotEmpty(host.CreatedAt.Load())
				assert.NotEmpty(host.UpdatedAt.Load())
				assert.NotNil(host.Log)
			},
		},
		{
			name:    "new host and set disk",
			rawHost: mockRawHost,
			options: []HostOption{WithDisk(mockDisk)},
			expect: func(t *testing.T, host *Host) {
				assert := assert.New(t)
				assert.Equal(host.ID, mockRawHost.ID)
				assert.Equal(host.Type, types.HostTypeNormal)
				assert.Equal(host.Hostname, mockRawHost.Hostname)
				assert.Equal(host.IP, mockRawHost.IP)
				assert.Equal(host.Port, mockRawHost.Port)
				assert.Equal(host.DownloadPort, mockRawHost.DownloadPort)
				assert.Equal(host.ObjectStoragePort, int32(0))
				assert.EqualValues(host.Disk, mockDisk)
				assert.Equal(host.SchedulerClusterID, uint64(0))
				assert.Equal(host.ConcurrentUploadLimit.Load(), int32(config.DefaultPeerConcurrentUploadLimit))
				assert.Equal(host.ConcurrentUploadCount.Load(), int32(0))
				assert.Equal(host.UploadCount.Load(), int64(0))
				assert.Equal(host.UploadFailedCount.Load(), int64(0))
				assert.NotNil(host.Peers)
				assert.Equal(host.PeerCount.Load(), int32(0))
				assert.NotEmpty(host.CreatedAt.Load())
				assert.NotEmpty(host.UpdatedAt.Load())
				assert.NotNil(host.Log)
			},
		},
		{
			name:    "new host and set build",
			rawHost: mockRawHost,
			options: []HostOption{WithBuild(mockBuild)},
			expect: func(t *testing.T, host *Host) {
				assert := assert.New(t)
				assert.Equal(host.ID, mockRawHost.ID)
				assert.Equal(host.Type, types.HostTypeNormal)
				assert.Equal(host.Hostname, mockRawHost.Hostname)
				assert.Equal(host.IP, mockRawHost.IP)
				assert.Equal(host.Port, mockRawHost.Port)
				assert.Equal(host.DownloadPort, mockRawHost.DownloadPort)
				assert.Equal(host.ObjectStoragePort, int32(0))
				assert.EqualValues(host.Build, mockBuild)
				assert.Equal(host.SchedulerClusterID, uint64(0))
				assert.Equal(host.ConcurrentUploadLimit.Load(), int32(config.DefaultPeerConcurrentUploadLimit))
				assert.Equal(host.ConcurrentUploadCount.Load(), int32(0))
				assert.Equal(host.UploadCount.Load(), int64(0))
				assert.Equal(host.UploadFailedCount.Load(), int64(0))
				assert.NotNil(host.Peers)
				assert.Equal(host.PeerCount.Load(), int32(0))
				assert.NotEmpty(host.CreatedAt.Load())
				assert.NotEmpty(host.UpdatedAt.Load())
				assert.NotNil(host.Log)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, NewHost(
				tc.rawHost.ID, tc.rawHost.IP, tc.rawHost.Hostname,
				tc.rawHost.Port, tc.rawHost.DownloadPort, tc.rawHost.Type,
				tc.options...))
		})
	}
}

func TestHost_LoadPeer(t *testing.T) {
	tests := []struct {
		name    string
		rawHost Host
		peerID  string
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
			peerID:  idgen.PeerIDV1("0.0.0.0"),
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
			host := NewHost(
				tc.rawHost.ID, tc.rawHost.IP, tc.rawHost.Hostname,
				tc.rawHost.Port, tc.rawHost.DownloadPort, tc.rawHost.Type)
			mockTask := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, WithDigest(mockTaskDigest))
			mockPeer := NewPeer(mockPeerID, mockResourceConfig, mockTask, host)

			host.StorePeer(mockPeer)
			peer, loaded := host.LoadPeer(tc.peerID)
			tc.expect(t, peer, loaded)
		})
	}
}

func TestHost_StorePeer(t *testing.T) {
	tests := []struct {
		name    string
		rawHost Host
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
			host := NewHost(
				tc.rawHost.ID, tc.rawHost.IP, tc.rawHost.Hostname,
				tc.rawHost.Port, tc.rawHost.DownloadPort, tc.rawHost.Type)
			mockTask := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, WithDigest(mockTaskDigest))
			mockPeer := NewPeer(tc.peerID, mockResourceConfig, mockTask, host)

			host.StorePeer(mockPeer)
			peer, loaded := host.LoadPeer(tc.peerID)
			tc.expect(t, peer, loaded)
		})
	}
}

func TestHost_DeletePeer(t *testing.T) {
	tests := []struct {
		name    string
		rawHost Host
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
			host := NewHost(
				tc.rawHost.ID, tc.rawHost.IP, tc.rawHost.Hostname,
				tc.rawHost.Port, tc.rawHost.DownloadPort, tc.rawHost.Type)
			mockTask := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, WithDigest(mockTaskDigest))
			mockPeer := NewPeer(mockPeerID, mockResourceConfig, mockTask, host)

			host.StorePeer(mockPeer)
			host.DeletePeer(tc.peerID)
			tc.expect(t, host)
		})
	}
}

func TestHost_LeavePeers(t *testing.T) {
	tests := []struct {
		name    string
		rawHost Host
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
			host := NewHost(
				tc.rawHost.ID, tc.rawHost.IP, tc.rawHost.Hostname,
				tc.rawHost.Port, tc.rawHost.DownloadPort, tc.rawHost.Type)
			mockTask := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, WithDigest(mockTaskDigest))
			mockPeer := NewPeer(mockPeerID, mockResourceConfig, mockTask, host)

			tc.expect(t, host, mockPeer)
		})
	}
}

func TestHost_FreeUploadCount(t *testing.T) {
	tests := []struct {
		name    string
		rawHost Host
		options []HostOption
		expect  func(t *testing.T, host *Host, mockTask *Task, mockPeer *Peer)
	}{
		{
			name:    "get free upload load",
			rawHost: mockRawHost,
			expect: func(t *testing.T, host *Host, mockTask *Task, mockPeer *Peer) {
				assert := assert.New(t)
				mockSeedPeer := NewPeer(mockSeedPeerID, mockResourceConfig, mockTask, host)
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
			host := NewHost(
				tc.rawHost.ID, tc.rawHost.IP, tc.rawHost.Hostname,
				tc.rawHost.Port, tc.rawHost.DownloadPort, tc.rawHost.Type)
			mockTask := NewTask(mockTaskID, mockTaskURL, mockTaskTag, mockTaskApplication, commonv2.TaskType_DFDAEMON, mockTaskFilteredQueryParams, mockTaskHeader, mockTaskBackToSourceLimit, WithDigest(mockTaskDigest))
			mockPeer := NewPeer(mockPeerID, mockResourceConfig, mockTask, host)

			tc.expect(t, host, mockTask, mockPeer)
		})
	}
}
