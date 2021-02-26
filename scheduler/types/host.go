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
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"sync"
)

type HostType int

const (
	HostTypePeer = 1
	HostTypeCdn  = 2
)

type Host struct {
	scheduler.PeerHost

	Type        HostType  // peer / cdn
	peerTaskMap *sync.Map // Pid => PeerTask
	// ProducerLoad is the load of download services provided by the current node.
	totalUploadLoad     int32
	currentUploadLoad   int32
	totalDownloadLoad   int32
	currentDownloadLoad int32
	loadLock            *sync.Mutex
	// ServiceDownTime the down time of the peer service.
	ServiceDownTime int64
}

func CopyHost(h *Host) *Host {
	copyHost := *h
	copyHost.peerTaskMap = new(sync.Map)
	copyHost.loadLock = new(sync.Mutex)
	return &copyHost
}

func (h *Host) AddPeerTask(peerTask *PeerTask) {
	h.peerTaskMap.Store(peerTask.Pid, peerTask)
}

func (h *Host) DeletePeerTask(peerTaskId string) {
	if h == nil || h.peerTaskMap == nil {
		return
	}
	h.peerTaskMap.Delete(peerTaskId)
}

func (h *Host) GetPeerTaskNum() int32 {
	count := 0
	if h.peerTaskMap != nil {
		h.peerTaskMap.Range(func(key interface{}, value interface{}) bool {
			count++
			return true
		})
	}
	return int32(count)
}

func (h *Host) GetPeerTask(peerTaskId string) (peerTask *PeerTask) {
	v, _ := h.peerTaskMap.Load(peerTaskId)
	peerTask, _ = v.(*PeerTask)
	return
}

func (h *Host) SetTotalUploadLoad(load int32) {
	h.loadLock.Lock()
	defer h.loadLock.Unlock()
	h.totalUploadLoad = load
}

func (h *Host) AddUploadLoad(delta int32) {
	h.loadLock.Lock()
	defer h.loadLock.Unlock()
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
	if h.totalUploadLoad <= 0 {
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
