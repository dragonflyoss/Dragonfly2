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

package resource

import (
	"sync"
)

type HostManager interface {
	// Load return host for a key
	Load(string) (*Host, bool)

	// Store set host
	Store(*Host)

	// LoadOrStore returns host the key if present.
	// Otherwise, it stores and returns the given host.
	// The loaded result is true if the host was loaded, false if stored.
	LoadOrStore(*Host) (*Host, bool)

	// Delete deletes host for a key
	Delete(string)
}

type hostManager struct {
	// Host sync map
	*sync.Map
}

// New host manager interface
func newHostManager() HostManager {
	return &hostManager{&sync.Map{}}
}

func (h *hostManager) Load(key string) (*Host, bool) {
	rawHost, ok := h.Map.Load(key)
	if !ok {
		return nil, false
	}

	return rawHost.(*Host), ok
}

func (h *hostManager) Store(host *Host) {
	h.Map.Store(host.ID, host)
}

func (h *hostManager) LoadOrStore(host *Host) (*Host, bool) {
	rawHost, loaded := h.Map.LoadOrStore(host.ID, host)
	return rawHost.(*Host), loaded
}

func (h *hostManager) Delete(key string) {
	h.Map.Delete(key)
}
