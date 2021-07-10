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

package types

import (
	"sync"
)

type HostType int

const (
	PeerNodeHost HostType = iota + 1
	CDNNodeHost
)
const (
	CDNHostLoad  = 10
	PeerHostLoad = 4
)

type NodeHost struct {
	// ProducerLoad is the load of download services provided by the current node.
	lock sync.RWMutex
	// fixme can remove this uuid, use IP
	// uuid each time the daemon starts, it will generate a different uuid
	UUID string
	// IP peer host ip
	IP string
	// hostName peer host name
	HostName string
	// RPCPort rpc service port for peer
	RPCPort int32
	// DownloadPort piece downloading port for peer
	DownloadPort int32
	// Type host type cdn or peer
	HostType HostType
	// SecurityDomain security isolation domain for network
	SecurityDomain string
	// Location location path: area|country|province|city|...
	Location string
	// Idc idc where the peer host is located
	IDC string
	// NetTopology network device path: switch|router|...
	NetTopology       string
	TotalUploadLoad   int32
	CurrentUploadLoad int32
	peerNodeMap       map[string]*PeerNode
}

func (h *NodeHost) AddPeerNode(peerNode *PeerNode) {
	h.lock.Lock()
	defer h.lock.Unlock()
	h.peerNodeMap[peerNode.PeerID] = peerNode
}

func (h *NodeHost) DeletePeerNode(peerID string) {
	h.lock.Lock()
	defer h.lock.Unlock()
	delete(h.peerNodeMap, peerID)
}

func (h *NodeHost) GetPeerTaskNum() int {
	h.lock.RLock()
	defer h.lock.RUnlock()
	return len(h.peerNodeMap)
}

func (h *NodeHost) GetPeerNode(peerID string) (*PeerNode, bool) {
	h.lock.RLock()
	defer h.lock.RUnlock()
	peerNode, ok := h.peerNodeMap[peerID]
	return peerNode, ok
}

func (h *NodeHost) IncUploadLoad() {
	h.CurrentUploadLoad++
}

func (h *NodeHost) DesUploadLoad() {
	h.CurrentUploadLoad--
}

func (h *NodeHost) GetUploadLoadPercent() float64 {
	if h.TotalUploadLoad <= 0 {
		return 1.0
	}
	return float64(h.CurrentUploadLoad) / float64(h.TotalUploadLoad)
}

func (h *NodeHost) GetFreeUploadLoad() int32 {
	return h.TotalUploadLoad - h.CurrentUploadLoad
}

func IsCDNHost(host *NodeHost) bool {
	return host.HostType == CDNNodeHost
}

func IsPeerHost(host *NodeHost) bool {
	return host.HostType == PeerNodeHost
}
