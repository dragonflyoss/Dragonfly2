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

package mgr

import (
	"d7y.io/dragonfly/v2/scheduler/types"
	"sync"
)

type HostManager struct {
	data *sync.Map
}

func createHostManager() *HostManager {
	return &HostManager{
		data: new(sync.Map),
	}
}

func (m *HostManager) AddHost(host *types.Host) *types.Host {
	v, ok := m.data.Load(host.Uuid)
	if ok {
		return v.(*types.Host)
	}

	copyHost := types.CopyHost(host)
	m.CalculateLoad(copyHost)
	m.data.Store(host.Uuid, copyHost)
	return copyHost
}

func (m *HostManager) DeleteHost(uuid string) {
	m.data.Delete(uuid)
	return
}

func (m *HostManager) GetHost(uuid string) (h *types.Host, ok bool) {
	data, ok := m.data.Load(uuid)
	if !ok {
		return
	}
	h = data.(*types.Host)
	return
}

func (m *HostManager) CalculateLoad(host *types.Host) {
	if host.Type == types.HostTypePeer {
		host.SetTotalUploadLoad(3)
		host.SetTotalDownloadLoad(3)
	} else {
		host.SetTotalUploadLoad(4)
		host.SetTotalDownloadLoad(4)
	}
	return
}
