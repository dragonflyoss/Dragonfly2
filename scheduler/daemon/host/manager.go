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
	managerRPC "d7y.io/dragonfly/v2/pkg/rpc/manager"
	"d7y.io/dragonfly/v2/scheduler/daemon"
	"d7y.io/dragonfly/v2/scheduler/types/host"
)

type manager struct {
	hostMap sync.Map
}

var _ daemon.HostMgr = (*manager)(nil)

func NewManager() daemon.HostMgr {
	return &manager{}
}

func (m *manager) Add(host *host.NodeHost) {
	m.hostMap.Store(host.UUID, host)
}

func (m *manager) Delete(uuid string) {
	m.hostMap.Delete(uuid)
}

func (m *manager) Get(uuid string) (*host.NodeHost, bool) {
	host, ok := m.hostMap.Load(uuid)
	if !ok {
		return nil, false
	}
	return host.(*host.NodeHost), true
}

func (m *manager) GetOrAdd(host *host.NodeHost) (actual *host.NodeHost, loaded bool) {
	item, loaded := m.hostMap.LoadOrStore(host.UUID, host)
	if loaded {
		return item.(*host.NodeHost), true
	}
	return host, false
}

func (m *manager) OnNotify(scheduler *managerRPC.Scheduler) {
	for _, cdn := range scheduler.Cdns {
		securityDomain := ""
		if cdn.CdnCluster != nil && cdn.CdnCluster.SecurityGroup != nil {
			securityDomain = cdn.CdnCluster.SecurityGroup.Name
		}
		cdnHost := &host.NodeHost{
			UUID:           idgen.CDNUUID(cdn.HostName, cdn.Port),
			IP:             cdn.Ip,
			HostName:       cdn.HostName,
			RPCPort:        cdn.Port,
			DownloadPort:   cdn.DownloadPort,
			HostType:       host.CDNNodeHost,
			SecurityDomain: securityDomain,
			Location:       cdn.Location,
			IDC:            cdn.Idc,
			//NetTopology:       server.NetTopology,
			TotalUploadLoad:   host.CDNHostLoad,
			CurrentUploadLoad: 0,
		}
		m.GetOrAdd(cdnHost)
	}
}
