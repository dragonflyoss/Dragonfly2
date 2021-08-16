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
	"d7y.io/dragonfly/v2/scheduler/supervisor"
)

type manager struct {
	hostMap sync.Map
}

var _ supervisor.HostMgr = (*manager)(nil)

func NewManager() supervisor.HostMgr {
	return &manager{}
}

func (m *manager) Add(host *supervisor.PeerHost) {
	m.hostMap.Store(host.UUID, host)
}

func (m *manager) Delete(uuid string) {
	m.hostMap.Delete(uuid)
}

func (m *manager) Get(uuid string) (*supervisor.PeerHost, bool) {
	host, ok := m.hostMap.Load(uuid)
	if !ok {
		return nil, false
	}
	return host.(*supervisor.PeerHost), true
}

func (m *manager) OnNotify(dynconfig *config.DynconfigData) {
	for _, cdn := range dynconfig.CDNs {
		cdnHost := supervisor.NewCDNPeerHost(idgen.CDNUUID(cdn.HostName, cdn.Port), cdn.IP, cdn.HostName, cdn.Port, cdn.DownloadPort, cdn.SecurityGroup,
			cdn.Location, cdn.IDC, cdn.NetTopology, 100)
		m.hostMap.Store(cdnHost.UUID, cdnHost)
	}
}
