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

	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/scheduler/types"
)

type Type int

const (
	// ClientPeerHostType represents client peerhost for scheduler
	ClientPeerHostType Type = 1 << iota

	// CDNPeerHostType represents CDN peerhost for scheduler
	CDNPeerHostType
)

const (
	// DefaultCDNMaxUploadCount represents CDN default maximum number of uploaders
	DefaultCDNMaxUploadCount = 10

	// DefaultCDNMaxDownloadCount represents CDN default maximum number of downloaders
	DefaultCDNMaxDownloadCount = 10

	// DefaultClientMaxUploadCount represents client default maximum number of uploaders
	DefaultClientMaxUploadCount = 4

	// DefaultClientMaxUploadCount represents client default maximum number of downloaders
	DefaultClientMaxDownloadCount = 4
)

type PeerHost interface {
	AddPeerTask(*types.PeerTask)
	DeletePeerTask(string)
	GetPeerTask(string) (*types.PeerTask, bool)
	GetPeerTaskLength() int32
	SetCurrentUploadCount(int32)
	GetCurrentUploadCount() int32
	GetUploadPercent() float64
	GetFreeUploadCount() int32
	SetCurrentDownloadCount(int32)
	GetCurrentDownloadCount() int32
	GetDownloadPercent() float64
	GetFreeDownloadCount() int32
}

type peerHost struct {
	scheduler.PeerHost

	peerType             Type
	peerTasks            *sync.Map
	maxUploadCount       int32
	currentUploadCount   int32
	maxDownloadCount     int32
	currentDownloadCount int32
	mutex                *sync.Mutex

	startTime time.Time
	endTime   time.Time
}

func newPeerHost(p *peerHost) PeerHost {
	// Set default upload and download count
	if p.peerType == ClientPeerHostType {
		p.maxUploadCount = DefaultClientMaxUploadCount
		p.maxDownloadCount = DefaultClientMaxUploadCount
	} else {
		p.maxUploadCount = DefaultCDNMaxUploadCount
		p.maxDownloadCount = DefaultCDNMaxUploadCount
	}

	p.peerTasks = &sync.Map{}
	p.mutex = &sync.Mutex{}

	return p
}

func (p *peerHost) AddPeerTask(pt *types.PeerTask) {
	p.peerTasks.Store(pt.Pid, pt)
}

func (p *peerHost) DeletePeerTask(ID string) {
	p.peerTasks.Delete(ID)
}

func (p *peerHost) GetPeerTask(ID string) (*types.PeerTask, bool) {
	v, ok := p.peerTasks.Load(ID)
	if ok {
		return v.(*types.PeerTask), true
	}

	return nil, false
}

func (p *peerHost) GetPeerTaskLength() int32 {
	count := 0
	p.peerTasks.Range(func(key interface{}, value interface{}) bool {
		count++
		return true
	})

	return int32(count)
}

func (p *peerHost) SetCurrentUploadCount(n int32) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.currentUploadCount = n
}

func (p *peerHost) GetCurrentUploadCount() int32 {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	return p.currentUploadCount
}

func (p *peerHost) GetUploadPercent() float64 {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	return float64(p.currentUploadCount) / float64(p.maxUploadCount)
}

func (p *peerHost) GetFreeUploadCount() int32 {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	return p.maxUploadCount - p.currentUploadCount
}

func (p *peerHost) SetCurrentDownloadCount(n int32) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.currentDownloadCount = n
}

func (p *peerHost) GetCurrentDownloadCount() int32 {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	return p.currentDownloadCount
}

func (p *peerHost) GetDownloadPercent() float64 {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	return float64(p.currentDownloadCount) / float64(p.maxDownloadCount)
}

func (p *peerHost) GetFreeDownloadCount() int32 {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	return p.maxDownloadCount - p.currentDownloadCount
}
