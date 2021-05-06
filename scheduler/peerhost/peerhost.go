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

package peerhost

import (
	"sync"
	"time"

	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
)

type PeerHostType int

const (
	// ClientPeerHostType represents client peerhost for scheduler
	ClientPeerHostType = 1 << iota

	// CDNPeerHostType represents CDN peerhost for scheduler
	CDNPeerHostType
)

const (
	DefaultCDNMaxUploaders   = 10
	DefaultCDNMaxDownloaders = 10

	DefaultClientMaxUploaders   = 4
	DefaultClientMaxDownloaders = 4
)

type PeerHost struct {
	scheduler.PeerHost

	Type               PeerHostType
	peerTaskMap        *sync.Map
	maxUploaders       int
	currentUploaders   int
	maxDownloaders     int
	currentDownloaders int
	mutex              *sync.Mutex

	startTime time.Time
	endTime   time.Time
}

func newPeerHost(p *PeerHost) *PeerHost {
	if p.Type == ClientPeerHostType {
		p.maxUploaders = DefaultClientMaxUploaders
		p.maxDownloaders = DefaultClientMaxUploaders
	} else {
		p.maxUploaders = DefaultCDNMaxUploaders
		p.maxDownloaders = DefaultCDNMaxUploaders
	}

	p.peerTaskMap = &sync.Map{}
	p.mutex = &sync.Mutex{}

	return p
}

func (p *PeerHost) AddPeerTask(pt *PeerTask) {
	p.peerTaskMap.Store(pt.Pid, pt)
}

func (p *PeerHost) DeletePeerTask(ID string) {
	p.peerTaskMap.Delete(ID)
}

func (h *PeerHost) GetPeerTask(ID string) (*PeerTask, bool) {
	v, ok := h.peerTaskMap.Load(ID)
	if ok {
		return v.(*PeerTask), ok
	}

	return nil, false
}

func (h *PeerHost) GetPeerTaskLenght() int {
	count := 0
	if h.peerTaskMap != nil {
		h.peerTaskMap.Range(func(key interface{}, value interface{}) bool {
			count++
			return true
		})
	}

	return count
}

func (h *PeerHost) SetTotalUploadLoad(load int32) {
	h.loadLock.Lock()
	defer h.loadLock.Unlock()
	h.totalUploadLoad = load
}

func (h *PeerHost) AddUploadLoad(delta int32) {
	h.loadLock.Lock()
	defer h.loadLock.Unlock()
	logger.Infof("host[%s] type[%d] add UploadLoad [%d]", h.Uuid, h.Type, delta)
	h.currentUploadLoad += delta
}

func (h *PeerHost) GetUploadLoad() int32 {
	h.loadLock.Lock()
	defer h.loadLock.Unlock()
	return h.currentUploadLoad
}

func (h *PeerHost) GetUploadLoadPercent() float64 {
	h.loadLock.Lock()
	defer h.loadLock.Unlock()
	if h.totalUploadLoad <= 0 {
		return 1.0
	}
	return float64(h.currentUploadLoad) / float64(h.totalUploadLoad)
}

func (h *PeerHost) GetFreeUploadLoad() int32 {
	h.loadLock.Lock()
	defer h.loadLock.Unlock()
	return h.totalUploadLoad - h.currentUploadLoad
}

func (h *PeerHost) SetTotalDownloadLoad(load int32) {
	h.loadLock.Lock()
	defer h.loadLock.Unlock()
	h.totalDownloadLoad = load
}

func (h *PeerHost) AddDownloadLoad(delta int32) {
	h.loadLock.Lock()
	defer h.loadLock.Unlock()
	h.currentDownloadLoad += delta
}

func (h *PeerHost) GetDownloadLoad() int32 {
	h.loadLock.Lock()
	defer h.loadLock.Unlock()
	return h.currentDownloadLoad
}

func (h *PeerHost) GetDownloadLoadPercent() float64 {
	h.loadLock.Lock()
	defer h.loadLock.Unlock()
	if h.totalDownloadLoad <= 0 {
		return 1.0
	}
	return float64(h.currentDownloadLoad) / float64(h.totalDownloadLoad)
}

func (h *PeerHost) GetFreeDownloadLoad() int32 {
	h.loadLock.Lock()
	defer h.loadLock.Unlock()
	return h.totalDownloadLoad - h.currentDownloadLoad
}
