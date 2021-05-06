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

	"d7y.io/dragonfly/v2/scheduler/types"
)

const (
	HostLoadCDN  = 10
	HostLoadPeer = 4
)

type PeerHostManager struct {
	peerhosts *sync.Map
}

func New() *PeerHostManager {
	return &PeerHostManager{
		peerhosts: &sync.Map{},
	}
}

func (p *PeerHostManager) Add(host *types.Host) *types.Host {
	v, ok := p.peerhosts.Load(host.Uuid)
	if ok {
		return v.(*types.Host)
	}

	copyHost := types.CopyHost(host)
	p.CalculateLoad(copyHost)
	p.peerhosts.Store(host.Uuid, copyHost)

	return copyHost
}

func (p *PeerHostManager) Delete(uuid string) {
	p.peerhosts.Delete(uuid)
}

func (m *PeerHostManager) Get(uuid string) (*types.Host, bool) {
	data, ok := m.peerhosts.Load(uuid)
	if !ok {
		return nil, false
	}

	h, ok := data.(*types.Host)
	if !ok {
		return nil, false
	}

	return h, true
}

func (m *PeerHostManager) CalculateLoad(host *types.Host) {
	if host.Type == types.HostTypePeer {
		host.SetTotalUploadLoad(HostLoadPeer)
		host.SetTotalDownloadLoad(HostLoadPeer)
	} else {
		host.SetTotalUploadLoad(HostLoadCDN)
		host.SetTotalDownloadLoad(HostLoadCDN)
	}
}
