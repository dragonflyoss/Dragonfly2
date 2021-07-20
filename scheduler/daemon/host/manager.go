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

	"d7y.io/dragonfly/v2/internal/idgen"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/daemon"
	"d7y.io/dragonfly/v2/scheduler/types"
)

type manager struct {
	hostMap sync.Map
}

var _ daemon.HostMgr = (*manager)(nil)

func NewManager() daemon.HostMgr {
	return &manager{}
}

func (m *manager) Add(host *types.PeerHost) {
	m.hostMap.Store(host.UUID, host)
}

func (m *manager) Delete(uuid string) {
	m.hostMap.Delete(uuid)
}

func (m *manager) Get(uuid string) (*types.PeerHost, bool) {
	host, ok := m.hostMap.Load(uuid)
	if !ok {
		return nil, false
	}
	return host.(*types.PeerHost), true
}

func (m *manager) GetOrAdd(host *types.PeerHost) (actual *types.PeerHost, loaded bool) {
	item, loaded := m.hostMap.LoadOrStore(host.UUID, host)
	if loaded {
		return item.(*types.PeerHost), true
	}
	return host, false
}

func (m *manager) OnNotify(dynconfig *config.DynconfigData) {
	for _, cdn := range dynconfig.CDNs {
		cdnHost := &types.PeerHost{
			UUID:           idgen.CDNUUID(cdn.HostName, cdn.Port),
			IP:             cdn.IP,
			HostName:       cdn.HostName,
			RPCPort:        cdn.Port,
			DownloadPort:   cdn.DownloadPort,
			CDN:            true,
			SecurityDomain: cdn.SecurityGroup,
			Location:       cdn.Location,
			IDC:            cdn.IDC,
			NetTopology:    cdn.NetTopology,
			//TotalUploadLoad: types.CDNHostLoad,
			TotalUploadLoad: 100,
		}
		m.GetOrAdd(cdnHost)
	}
}
