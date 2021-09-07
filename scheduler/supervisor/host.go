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

package supervisor

import (
	"sync"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"go.uber.org/atomic"
)

type PeerHost struct {
	lock sync.RWMutex
	// uuid each time the daemon starts, it will generate a different uuid
	UUID string
	// IP peer host ip
	IP string
	// HostName peer host name
	HostName string
	// RPCPort rpc service port for peer
	RPCPort int32
	// DownloadPort piece downloading port for peer
	DownloadPort int32
	// CDN if host type is cdn
	CDN bool
	// SecurityDomain security isolation domain for network
	SecurityDomain string
	// Location location path: area|country|province|city|...
	Location string
	// IDC idc where the peer host is located
	IDC string
	// NetTopology network device path: switch|router|...
	NetTopology string
	// TODO TotalUploadLoad currentUploadLoad decided by real time client report host info
	TotalUploadLoad   int32
	currentUploadLoad atomic.Int32
	peerMap           map[string]*Peer
	logger            *logger.SugaredLoggerOnWith
}

func NewClientPeerHost(uuid, ip, hostname string, rpcPort, downloadPort int32, securityDomain, location, idc, netTopology string,
	totalUploadLoad int32) *PeerHost {
	return newPeerHost(uuid, ip, hostname, rpcPort, downloadPort, false, securityDomain, location, idc, netTopology, totalUploadLoad)
}

func NewCDNPeerHost(uuid, ip, hostname string, rpcPort, downloadPort int32, securityDomain, location, idc, netTopology string,
	totalUploadLoad int32) *PeerHost {
	return newPeerHost(uuid, ip, hostname, rpcPort, downloadPort, true, securityDomain, location, idc, netTopology, totalUploadLoad)
}

func newPeerHost(uuid, ip, hostname string, rpcPort, downloadPort int32, isCDN bool, securityDomain, location, idc, netTopology string,
	totalUploadLoad int32) *PeerHost {
	return &PeerHost{
		UUID:            uuid,
		IP:              ip,
		HostName:        hostname,
		RPCPort:         rpcPort,
		DownloadPort:    downloadPort,
		CDN:             isCDN,
		SecurityDomain:  securityDomain,
		Location:        location,
		IDC:             idc,
		NetTopology:     netTopology,
		TotalUploadLoad: totalUploadLoad,
		peerMap:         make(map[string]*Peer),
		logger:          logger.With("hostUUID", uuid),
	}
}

func (h *PeerHost) AddPeer(peer *Peer) {
	h.lock.Lock()
	defer h.lock.Unlock()
	h.peerMap[peer.PeerID] = peer
}

func (h *PeerHost) DeletePeer(peerID string) {
	h.lock.Lock()
	defer h.lock.Unlock()
	delete(h.peerMap, peerID)
}

func (h *PeerHost) GetPeerTaskNum() int {
	h.lock.RLock()
	defer h.lock.RUnlock()
	return len(h.peerMap)
}

func (h *PeerHost) GetPeer(peerID string) (*Peer, bool) {
	h.lock.RLock()
	defer h.lock.RUnlock()
	peer, ok := h.peerMap[peerID]
	return peer, ok
}

func (h *PeerHost) GetCurrentUpload() int32 {
	return h.currentUploadLoad.Load()
}

func (h *PeerHost) GetUploadLoadPercent() float64 {
	if h.TotalUploadLoad <= 0 {
		return 1.0
	}
	return float64(h.currentUploadLoad.Load()) / float64(h.TotalUploadLoad)
}

func (h *PeerHost) GetFreeUploadLoad() int {
	return int(h.TotalUploadLoad - h.currentUploadLoad.Load())
}

func (h *PeerHost) IncUploadLoad() int32 {
	return h.currentUploadLoad.Inc()
}

func (h *PeerHost) DecUploadLoad() int32 {
	return h.currentUploadLoad.Dec()
}

func (h *PeerHost) Log() *logger.SugaredLoggerOnWith {
	h.lock.RLock()
	if h.logger != nil {
		h.lock.RUnlock()
		return h.logger
	}
	h.lock.RUnlock()
	h.lock.Lock()
	defer h.lock.Unlock()
	if h.logger == nil {
		h.logger = logger.WithTaskID(h.UUID)
	}
	return h.logger
}
