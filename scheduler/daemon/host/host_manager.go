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

const (
	CDNHostLoad  = 10
	PeerHostLoad = 4
)

type manager struct {
	data sync.Map
}

func (m *manager) LoadOrStore(uuid string, host *types.Host) (*types.Host, bool) {
	panic("implement me")
}

func newHostManager() daemon.HostMgr {
	return &manager{}
}

var _ daemon.HostMgr = (*manager)(nil)

func (m *manager) Store(uuid string, host *types.Host) *types.Host {
	v, ok := m.data.Load(uuid)
	if ok {
		return v.(*types.Host)
	}

	h := types.Init(host)
	if host.Type == types.HostTypePeer {
		host.SetTotalUploadLoad(PeerHostLoad)
		host.SetTotalDownloadLoad(PeerHostLoad)
	}
	host.SetTotalUploadLoad(CDNHostLoad)
	host.SetTotalDownloadLoad(CDNHostLoad)
	m.data.Store(host.Uuid, h)

	return h
}

func (m *manager) Delete(uuid string) {
	m.data.Delete(uuid)
}

func (m *manager) Load(uuid string) (*types.Host, bool) {
	data, ok := m.data.Load(uuid)
	if !ok {
		return nil, false
	}

	h, ok := data.(*types.Host)
	if !ok {
		return nil, false
	}

	return h, true
}
