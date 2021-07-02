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
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"go.uber.org/atomic"
)

type HostType int

const (
	PeerNodeHost = iota + 1
	CDNNodeHost
)

type NodeHost struct {
	// fixme can remove this uuid, use IP
	// uuid each time the daemon starts, it will generate a different uuid
	uuid string
	// IP peer host ip
	ip string
	// hostName peer host name
	hostName string
	// rpcPort rpc service port for peer
	rpcPort int32
	// downloadPort piece downloading port for peer
	downloadPort int32
	// Type host type cdn or peer
	hostType HostType
	// SecurityDomain security isolation domain for network
	SecurityDomain string
	// Location location path: area|country|province|city|...
	Location string
	// Idc idc where the peer host is located
	Idc string
	// NetTopology network device path: switch|router|...
	NetTopology string
	// ProducerLoad is the load of download services provided by the current node.
	TotalUploadLoad     int
	currentUploadLoad   atomic.Int32
	totalDownloadLoad   int32
	currentDownloadLoad atomic.Int32
}

func NewNodeHost() *NodeHost {
	return &NodeHost{}
}

func (h *NodeHost) GetUUID() string {
	return h.UUID
}

func (h *NodeHost) AddPeerNode(peerNode *PeerNode) {
	h.peerTaskMap.Store(peerNode.GetPeerID(), peerNode)
}

func (h *NodeHost) DeletePeerNode(peerID string) {
	h.peerTaskMap.Delete(peerID)
}

func (h *NodeHost) GetPeerTaskNum() int32 {
	count := 0
	h.peerTaskMap.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return int32(count)
}

func (h *NodeHost) GetPeerNode(peerID string) (*PeerNode, bool) {
	v, ok := h.peerTaskMap.Load(peerID)
	if !ok {
		return nil, false
	}
	return v.(*PeerNode), true
}

func (h *NodeHost) SetTotalUploadLoad(load int32) {
	h.totalUploadLoad = load
}

func (h *NodeHost) AddUploadLoad(delta int32) {
	logger.Infof("host[%s] type[%d] add UploadLoad [%d]", h.UUID, h.Type, delta)
	h.currentUploadLoad += delta
}

func (h *NodeHost) GetUploadLoad() int {
	return h.currentUploadLoad
}

func (h *NodeHost) GetUploadLoadPercent() float64 {
	if h.TotalUploadLoad <= 0 {
		return 1.0
	}
	return float64(h.currentUploadLoad.Load()) / float64(h.TotalUploadLoad)
}

func (h *NodeHost) GetFreeUploadLoad() int32 {
	return h.totalUploadLoad - h.currentUploadLoad
}

func (h *NodeHost) SetTotalDownloadLoad(load int) {
	h.totalDownloadLoad = load
}

func (h *NodeHost) AddDownloadLoad(delta int) {
	h.currentDownloadLoad += delta
}

func (h *NodeHost) GetDownloadLoad() int32 {
	return h.currentDownloadLoad
}

func (h *NodeHost) GetDownloadLoadPercent() float64 {
	if h.totalDownloadLoad <= 0 {
		return 1.0
	}
	return float64(h.currentDownloadLoad) / float64(h.totalDownloadLoad)
}

func (h *NodeHost) GetFreeDownloadLoad() int32 {
	return h.totalDownloadLoad - h.currentDownloadLoad
}

func IsCDN(host *NodeHost) bool {
	return host.Type == CDNNodeHost
}
