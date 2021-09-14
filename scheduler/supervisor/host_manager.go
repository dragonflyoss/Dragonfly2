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

package supervisor

import (
	"sync"
)

type HostManager interface {
	Add(*Host)

	Delete(string)

	Get(string) (*Host, bool)
}

type hostManager struct {
	*sync.Map
}

func NewHostManager() HostManager {
	return &hostManager{&sync.Map{}}
}

func (m *hostManager) Get(key string) (*Host, bool) {
	host, ok := m.Load(key)
	if !ok {
		return nil, false
	}

	return host.(*Host), true
}

func (m *hostManager) Add(host *Host) {
	m.Store(host.UUID, host)
}

func (m *hostManager) Delete(key string) {
	m.Map.Delete(key)
}
