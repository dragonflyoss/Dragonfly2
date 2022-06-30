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
	"sync"
	"time"

	"go.uber.org/atomic"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/scheduler/config"
)

type HostType int

const (
	// HostTypeNormal is the normal type of host.
	HostTypeNormal HostType = iota

	// HostTypeSuperSeed is the super seed type of host.
	HostTypeSuperSeed

	// HostTypeStrongSeed is the strong seed type of host.
	HostTypeStrongSeed

	// HostTypeWeakSeed is the weak seed type of host.
	HostTypeWeakSeed
)

// HostOption is a functional option for configuring the host.
type HostOption func(h *Host) *Host

// WithUploadLoadLimit sets host's UploadLoadLimit.
func WithUploadLoadLimit(limit int32) HostOption {
	return func(h *Host) *Host {
		h.UploadLoadLimit.Store(limit)
		return h
	}
}

// WithHostType sets host's type.
func WithHostType(hostType HostType) HostOption {
	return func(h *Host) *Host {
		h.Type = hostType
		return h
	}
}

type Host struct {
	// ID is host id.
	ID string

	// Type is host type.
	Type HostType

	// IP is host ip.
	IP string

	// Hostname is host name.
	Hostname string

	// Port is grpc service port.
	Port int32

	// DownloadPort is piece downloading port.
	DownloadPort int32

	// SecurityDomain is security domain of host.
	SecurityDomain string

	// IDC is internet data center of host.
	IDC string

	// NetTopology is network topology of host.
	// Example: switch|router|...
	NetTopology string

	// Location is location of host.
	// Example: country|province|...
	Location string

	// UploadLoadLimit is upload load limit count.
	UploadLoadLimit *atomic.Int32

	// UploadPeerCount is upload peer count.
	UploadPeerCount *atomic.Int32

	// Peer sync map.
	Peers *sync.Map

	// PeerCount is peer count.
	PeerCount *atomic.Int32

	// CreateAt is host create time.
	CreateAt *atomic.Time

	// UpdateAt is host update time.
	UpdateAt *atomic.Time

	// Host log.
	Log *logger.SugaredLoggerOnWith
}

// New host instance.
func NewHost(rawHost *scheduler.PeerHost, options ...HostOption) *Host {
	h := &Host{
		ID:              rawHost.Id,
		Type:            HostTypeNormal,
		IP:              rawHost.Ip,
		Hostname:        rawHost.HostName,
		Port:            rawHost.RpcPort,
		DownloadPort:    rawHost.DownPort,
		SecurityDomain:  rawHost.SecurityDomain,
		IDC:             rawHost.Idc,
		NetTopology:     rawHost.NetTopology,
		Location:        rawHost.Location,
		UploadLoadLimit: atomic.NewInt32(config.DefaultClientLoadLimit),
		UploadPeerCount: atomic.NewInt32(0),
		Peers:           &sync.Map{},
		PeerCount:       atomic.NewInt32(0),
		CreateAt:        atomic.NewTime(time.Now()),
		UpdateAt:        atomic.NewTime(time.Now()),
		Log:             logger.WithHostID(rawHost.Id),
	}

	for _, opt := range options {
		opt(h)
	}

	return h
}

// LoadPeer return peer for a key.
func (h *Host) LoadPeer(key string) (*Peer, bool) {
	rawPeer, ok := h.Peers.Load(key)
	if !ok {
		return nil, false
	}

	return rawPeer.(*Peer), ok
}

// StorePeer set peer.
func (h *Host) StorePeer(peer *Peer) {
	h.Peers.Store(peer.ID, peer)
	h.PeerCount.Inc()
}

// LoadOrStorePeer returns peer the key if present.
// Otherwise, it stores and returns the given peer.
// The loaded result is true if the peer was loaded, false if stored.
func (h *Host) LoadOrStorePeer(peer *Peer) (*Peer, bool) {
	rawPeer, loaded := h.Peers.LoadOrStore(peer.ID, peer)
	if !loaded {
		h.PeerCount.Inc()
	}

	return rawPeer.(*Peer), loaded
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
		if peer, ok := value.(*Peer); ok {
			if err := peer.FSM.Event(PeerEventDownloadFailed); err != nil {
				peer.Log.Errorf("peer fsm event failed: %s", err.Error())
				return true
			}

			if err := peer.FSM.Event(PeerEventLeave); err != nil {
				peer.Log.Errorf("peer fsm event failed: %s", err.Error())
				return true
			}

			h.Log.Infof("peer %s has been left", peer.ID)
		}

		return true
	})
}

// FreeUploadLoad return free upload load of host.
func (h *Host) FreeUploadLoad() int32 {
	return h.UploadLoadLimit.Load() - h.UploadPeerCount.Load()
}
