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

package host

import (
	"sync"

	"d7y.io/dragonfly/v2/scheduler/daemon"
	"d7y.io/dragonfly/v2/scheduler/types"
)

const ()

type manager struct {
	hostMap sync.Map
}

var _ daemon.HostMgr = (*manager)(nil)

func newHostManager() daemon.HostMgr {
	return &manager{}
}

func (m *manager) Add(host *types.Host) {
	if host.Type == types.HostTypePeer {
		host.SetTotalUploadLoad(PeerHostLoad)
		host.SetTotalDownloadLoad(PeerHostLoad)
	}
	host.SetTotalUploadLoad(CDNHostLoad)
	host.SetTotalDownloadLoad(CDNHostLoad)
	m.hostMap.Store(host.UUID, host)
}

func (m *manager) Delete(uuid string) {
	m.hostMap.Delete(uuid)
}

func (m *manager) Get(uuid string) (*types.Host, bool) {
	host, ok := m.hostMap.Load(uuid)
	if !ok {
		return nil, false
	}
	return host.(*types.Host), true
}

func (m *manager) GetOrAdd(uuid string, host *types.Host) (*types.Host, bool) {
	item, loaded := m.hostMap.LoadOrStore(uuid, host)
	if loaded {
		return item.(*types.Host), true
	}
	return host, false
}
