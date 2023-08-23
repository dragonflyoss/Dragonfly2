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
	"sync"
	"time"

	"go.uber.org/atomic"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/types"
	"d7y.io/dragonfly/v2/scheduler/config"
)

// HostOption is a functional option for configuring the host.
type HostOption func(h *Host)

// WithSchedulerClusterID sets host's SchedulerClusterID.
func WithSchedulerClusterID(id uint64) HostOption {
	return func(h *Host) {
		h.SchedulerClusterID = id
	}
}

// WithObjectStoragePort sets host's ObjectStoragePort.
func WithObjectStoragePort(port int32) HostOption {
	return func(h *Host) {
		h.ObjectStoragePort = port
	}
}

// WithConcurrentUploadLimit sets host's ConcurrentUploadLimit.
func WithConcurrentUploadLimit(limit int32) HostOption {
	return func(h *Host) {
		h.ConcurrentUploadLimit.Store(limit)
	}
}

// WithOS sets host's os.
func WithOS(os string) HostOption {
	return func(h *Host) {
		h.OS = os
	}
}

// WithPlatform sets host's platform.
func WithPlatform(platform string) HostOption {
	return func(h *Host) {
		h.Platform = platform
	}
}

// WithPlatformFamily sets host's platform family.
func WithPlatformFamily(platformFamily string) HostOption {
	return func(h *Host) {
		h.PlatformFamily = platformFamily
	}
}

// WithPlatformVersion sets host's platform version.
func WithPlatformVersion(platformVersion string) HostOption {
	return func(h *Host) {
		h.PlatformVersion = platformVersion
	}
}

// WithKernelVersion sets host's kernel version.
func WithKernelVersion(kernelVersion string) HostOption {
	return func(h *Host) {
		h.KernelVersion = kernelVersion
	}
}

// WithCPU sets host's cpu.
func WithCPU(cpu CPU) HostOption {
	return func(h *Host) {
		h.CPU = cpu
	}
}

// WithMemory sets host's memory.
func WithMemory(memory Memory) HostOption {
	return func(h *Host) {
		h.Memory = memory
	}
}

// WithNetwork sets host's network.
func WithNetwork(network Network) HostOption {
	return func(h *Host) {
		h.Network = network
	}
}

// WithDisk sets host's disk.
func WithDisk(disk Disk) HostOption {
	return func(h *Host) {
		h.Disk = disk
	}
}

// WithBuild sets host's build information.
func WithBuild(build Build) HostOption {
	return func(h *Host) {
		h.Build = build
	}
}

// Host contains content for host.
type Host struct {
	// ID is host id.
	ID string

	// Type is host type.
	Type types.HostType

	// Hostname is host name.
	Hostname string

	// IP is host ip.
	IP string

	// Port is grpc service port.
	Port int32

	// DownloadPort is piece downloading port.
	DownloadPort int32

	// ObjectStoragePort is object storage port.
	ObjectStoragePort int32

	// Host OS.
	OS string

	// Host platform.
	Platform string

	// Host platform family.
	PlatformFamily string

	// Host platform version.
	PlatformVersion string

	// Host kernel version.
	KernelVersion string

	// CPU Stat.
	CPU CPU

	// Memory Stat.
	Memory Memory

	// Network Stat.
	Network Network

	// Dist Stat.
	Disk Disk

	// Build information.
	Build Build

	// SchedulerClusterID is the scheduler cluster id matched by scopes.
	SchedulerClusterID uint64

	// ConcurrentUploadLimit is concurrent upload limit count.
	ConcurrentUploadLimit *atomic.Int32

	// ConcurrentUploadCount is concurrent upload count.
	ConcurrentUploadCount *atomic.Int32

	// UploadCount is total upload count.
	UploadCount *atomic.Int64

	// UploadFailedCount is upload failed count.
	UploadFailedCount *atomic.Int64

	// Peer sync map.
	Peers *sync.Map

	// PeerCount is peer count.
	PeerCount *atomic.Int32

	// CreatedAt is host create time.
	CreatedAt *atomic.Time

	// UpdatedAt is host update time.
	UpdatedAt *atomic.Time

	// Host log.
	Log *logger.SugaredLoggerOnWith
}

// CPU contains content for cpu.
type CPU struct {
	// Number of logical cores in the system.
	LogicalCount uint32 `csv:"logicalCount"`

	// Number of physical cores in the system.
	PhysicalCount uint32 `csv:"physicalCount"`

	// Percent calculates the percentage of cpu used.
	Percent float64 `csv:"percent"`

	// Calculates the percentage of cpu used by process.
	ProcessPercent float64 `csv:"processPercent"`

	// Times contains the amounts of time the CPU has spent performing different kinds of work.
	Times CPUTimes `csv:"times"`
}

// CPUTimes contains content for cpu times.
type CPUTimes struct {
	// CPU time of user.
	User float64 `csv:"user"`

	// CPU time of system.
	System float64 `csv:"system"`

	// CPU time of idle.
	Idle float64 `csv:"idle"`

	// CPU time of nice.
	Nice float64 `csv:"nice"`

	// CPU time of iowait.
	Iowait float64 `csv:"iowait"`

	// CPU time of irq.
	Irq float64 `csv:"irq"`

	// CPU time of softirq.
	Softirq float64 `csv:"softirq"`

	// CPU time of steal.
	Steal float64 `csv:"steal"`

	// CPU time of guest.
	Guest float64 `csv:"guest"`

	// CPU time of guest nice.
	GuestNice float64 `csv:"guestNice"`
}

// Memory contains content for memory.
type Memory struct {
	// Total amount of RAM on this system.
	Total uint64 `csv:"total"`

	// RAM available for programs to allocate.
	Available uint64 `csv:"available"`

	// RAM used by programs.
	Used uint64 `csv:"used"`

	// Percentage of RAM used by programs.
	UsedPercent float64 `csv:"usedPercent"`

	// Calculates the percentage of memory used by process.
	ProcessUsedPercent float64 `csv:"processUsedPercent"`

	// This is the kernel's notion of free memory.
	Free uint64 `csv:"free"`
}

// Network contains content for network.
type Network struct {
	// Return count of tcp connections opened and status is ESTABLISHED.
	TCPConnectionCount uint32 `csv:"tcpConnectionCount"`

	// Return count of upload tcp connections opened and status is ESTABLISHED.
	UploadTCPConnectionCount uint32 `csv:"uploadTCPConnectionCount"`

	// Location path(area|country|province|city|...).
	Location string `csv:"location"`

	// IDC where the peer host is located
	IDC string `csv:"idc"`
}

// Build contains content for build.
type Build struct {
	// Git version.
	GitVersion string `csv:"gitVersion"`

	// Git commit.
	GitCommit string `csv:"gitCommit"`

	// Golang version.
	GoVersion string `csv:"goVersion"`

	// Build platform.
	Platform string `csv:"platform"`
}

// Disk contains content for disk.
type Disk struct {
	// Total amount of disk on the data path of dragonfly.
	Total uint64 `csv:"total"`

	// Free amount of disk on the data path of dragonfly.
	Free uint64 `csv:"free"`

	// Used amount of disk on the data path of dragonfly.
	Used uint64 `csv:"used"`

	// Used percent of disk on the data path of dragonfly directory.
	UsedPercent float64 `csv:"usedPercent"`

	// Total amount of indoes on the data path of dragonfly directory.
	InodesTotal uint64 `csv:"inodesTotal"`

	// Used amount of indoes on the data path of dragonfly directory.
	InodesUsed uint64 `csv:"inodesUsed"`

	// Free amount of indoes on the data path of dragonfly directory.
	InodesFree uint64 `csv:"inodesFree"`

	// Used percent of indoes on the data path of dragonfly directory.
	InodesUsedPercent float64 `csv:"inodesUsedPercent"`
}

// New host instance.
func NewHost(
	id, ip, hostname string, port, downloadPort int32,
	typ types.HostType, options ...HostOption,
) *Host {
	// Calculate default of the concurrent upload limit by host type.
	concurrentUploadLimit := config.DefaultSeedPeerConcurrentUploadLimit
	if typ == types.HostTypeNormal {
		concurrentUploadLimit = config.DefaultPeerConcurrentUploadLimit
	}

	h := &Host{
		ID:                    id,
		Type:                  types.HostType(typ),
		IP:                    ip,
		Hostname:              hostname,
		Port:                  port,
		DownloadPort:          downloadPort,
		ConcurrentUploadLimit: atomic.NewInt32(int32(concurrentUploadLimit)),
		ConcurrentUploadCount: atomic.NewInt32(0),
		UploadCount:           atomic.NewInt64(0),
		UploadFailedCount:     atomic.NewInt64(0),
		Peers:                 &sync.Map{},
		PeerCount:             atomic.NewInt32(0),
		CreatedAt:             atomic.NewTime(time.Now()),
		UpdatedAt:             atomic.NewTime(time.Now()),
		Log:                   logger.WithHost(id, hostname, ip),
	}

	for _, opt := range options {
		opt(h)
	}

	return h
}

// LoadPeer return peer for a key.
func (h *Host) LoadPeer(key string) (*Peer, bool) {
	rawPeer, loaded := h.Peers.Load(key)
	if !loaded {
		return nil, false
	}

	return rawPeer.(*Peer), loaded
}

// StorePeer set peer.
func (h *Host) StorePeer(peer *Peer) {
	h.Peers.Store(peer.ID, peer)
	h.PeerCount.Inc()
}

// DeletePeer deletes peer for a key.
func (h *Host) DeletePeer(key string) {
	if _, loaded := h.Peers.LoadAndDelete(key); loaded {
		h.PeerCount.Dec()
	}
}

// LeavePeers set peer state to PeerStateLeave.
func (h *Host) LeavePeers() {
	h.Peers.Range(func(_, value any) bool {
		peer, ok := value.(*Peer)
		if !ok {
			h.Log.Error("invalid peer")
			return true
		}

		peer.Log.Info("host leaves peers, causing the peer to leave")
		if err := peer.FSM.Event(context.Background(), PeerEventLeave); err != nil {
			peer.Log.Errorf("peer fsm event failed: %s", err.Error())
			return true
		}

		return true
	})
}

// FreeUploadCount return free upload count of host.
func (h *Host) FreeUploadCount() int32 {
	return h.ConcurrentUploadLimit.Load() - h.ConcurrentUploadCount.Load()
}
