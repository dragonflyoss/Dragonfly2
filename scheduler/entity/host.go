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

package entity

import (
	"sync"

	"go.uber.org/atomic"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
)

const (
	defaultUploadLoadLimit = 100
)

type HostOption func(rt *Host) *Host

func WithTotalUploadLoad(load uint32) HostOption {
	return func(h *Host) *Host {
		h.UploadLoadLimit = load
		return h
	}
}

func WithIsCDN(isCDN bool) HostOption {
	return func(h *Host) *Host {
		h.IsCDN = isCDN
		return h
	}
}

type Host struct {
	// ID is host id
	ID string

	// IP is host ip
	IP string

	// HostName is host name
	HostName string

	// Port is grpc service port
	Port int32

	// DownloadPort is piece downloading port
	DownloadPort int32

	// SecurityDomain is security domain of host
	SecurityDomain string

	// IDC is internet data center of host
	IDC string

	// NetTopology is network topology of host
	// Example: switch|router|...
	NetTopology string

	// Location is location of host
	Location string

	// IsCDN is used as tag cdn
	IsCDN bool

	// UploadLoad is current upload load count
	UploadLoad atomic.Uint32

	// UploadLoadLimit is upload load limit count
	UploadLoadLimit uint32

	// Peer sync map
	peers *sync.Map

	// Host logger
	logger *logger.SugaredLoggerOnWith
}

func NewHost(rawHost *scheduler.PeerHost, options ...HostOption) *Host {
	h := &Host{
		ID:              rawHost.Uuid,
		IP:              rawHost.Ip,
		HostName:        rawHost.HostName,
		Port:            rawHost.RpcPort,
		DownloadPort:    rawHost.DownPort,
		SecurityDomain:  rawHost.SecurityDomain,
		IDC:             rawHost.Idc,
		NetTopology:     rawHost.NetTopology,
		Location:        rawHost.Location,
		IsCDN:           false,
		UploadLoadLimit: defaultUploadLoadLimit,
		peers:           &sync.Map{},
		logger:          logger.With("hostID", rawHost.Uuid),
	}

	for _, opt := range options {
		opt(h)
	}

	return h
}

func (h *Host) LoadPeer(key string) (*Peer, bool) {
	rawPeer, ok := h.peers.Load(key)
	if !ok {
		return nil, false
	}

	return rawPeer.(*Peer), ok
}

func (h *Host) StorePeer(peer *Peer) {
	h.peers.Store(peer.ID, peer)
}

func (h *Host) LoadOrStorePeer(peer *Peer) (*Peer, bool) {
	rawPeer, loaded := h.peers.LoadOrStore(peer.ID, peer)
	return rawPeer.(*Peer), loaded
}

func (h *Host) DeletePeer(key string) {
	h.peers.Delete(key)
}

func (h *Host) LenPeers() int {
	var len int
	h.peers.Range(func(_, _ interface{}) bool {
		len++
		return true
	})

	return len
}

func (h *Host) FreeUploadLoad() uint32 {
	return h.UploadLoadLimit - h.UploadLoad.Load()
}

func (h *Host) Log() *logger.SugaredLoggerOnWith {
	return h.logger
}
