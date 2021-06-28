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

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"go.uber.org/atomic"
)

type HostType int32

const (
	HostTypePeer = iota + 1
	HostTypeCdn
)

type Host struct {
	// fixme can remove this uuid, use IP
	// UUID each time the daemon starts, it will generate a different uuid
	UUID string
	// IP peer host ip
	IP string
	// HostName peer host name
	HostName string
	// RpcPort rpc service port for peer
	RpcPort int32
	// DownloadPort piece downloading port for peer
	DownloadPort int32
	// SecurityDomain security isolation domain for network
	SecurityDomain string
	// Location location path: area|country|province|city|...
	Location string
	// Idc idc where the peer host is located
	Idc string
	// NetTopology network device path: switch|router|...
	NetTopology string
	// Type host type cdn or peer
	Type        HostType
	peerTaskMap sync.Map // Pid => PeerTask
	// ProducerLoad is the load of download services provided by the current node.
	TotalUploadLoad     int32
	currentUploadLoad   atomic.Int32
	totalDownloadLoad   int32
	currentDownloadLoad atomic.Int32
}

func (h *Host) AddPeerTask(peerNode *PeerNode) {
	h.peerTaskMap.Store(peerNode.Pid, peerNode)
}

func (h *Host) DeletePeerTask(peerTaskID string) {
	h.peerTaskMap.Delete(peerTaskID)
}

func (h *Host) GetPeerTaskNum() int32 {
	count := 0
	h.peerTaskMap.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return int32(count)
}

func (h *Host) GetPeerTask(peerID string) (peerTask *PeerTask) {
	v, _ := h.peerTaskMap.Load(peerID)
	peerTask, _ = v.(*PeerTask)
	return
}

func (h *Host) SetTotalUploadLoad(load int32) {
	h.totalUploadLoad = load
}

func (h *Host) AddUploadLoad(delta int32) {
	h.loadLock.Lock()
	defer h.loadLock.Unlock()
	logger.Infof("host[%s] type[%d] add UploadLoad [%d]", h.Uuid, h.Type, delta)
	h.currentUploadLoad += delta
}

func (h *Host) GetUploadLoad() int32 {
	h.loadLock.Lock()
	defer h.loadLock.Unlock()
	return h.currentUploadLoad
}

func (h *Host) GetUploadLoadPercent() float64 {
	h.loadLock.Lock()
	defer h.loadLock.Unlock()
	if h.TotalUploadLoad <= 0 {
		return 1.0
	}
	return float64(h.currentUploadLoad) / float64(h.totalUploadLoad)
}

func (h *Host) GetFreeUploadLoad() int32 {
	h.loadLock.Lock()
	defer h.loadLock.Unlock()
	return h.totalUploadLoad - h.currentUploadLoad
}

func (h *Host) SetTotalDownloadLoad(load int32) {
	h.loadLock.Lock()
	defer h.loadLock.Unlock()
	h.totalDownloadLoad = load
}

func (h *Host) AddDownloadLoad(delta int32) {
	h.loadLock.Lock()
	defer h.loadLock.Unlock()
	h.currentDownloadLoad += delta
}

func (h *Host) GetDownloadLoad() int32 {
	h.loadLock.Lock()
	defer h.loadLock.Unlock()
	return h.currentDownloadLoad
}

func (h *Host) GetDownloadLoadPercent() float64 {
	h.loadLock.Lock()
	defer h.loadLock.Unlock()
	if h.totalDownloadLoad <= 0 {
		return 1.0
	}
	return float64(h.currentDownloadLoad) / float64(h.totalDownloadLoad)
}

func (h *Host) GetFreeDownloadLoad() int32 {
	h.loadLock.Lock()
	defer h.loadLock.Unlock()
	return h.totalDownloadLoad - h.currentDownloadLoad
}
