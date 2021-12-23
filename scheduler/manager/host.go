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

package manager

import (
	"sync"

	"d7y.io/dragonfly/v2/scheduler/entity"
)

type Host interface {
	// Load return host entity for a key
	Load(string) (*entity.Host, bool)

	// Store set host entity
	Store(*entity.Host)

	// LoadOrStore returns host entity the key if present.
	// Otherwise, it stores and returns the given host entity.
	// The loaded result is true if the host entity was loaded, false if stored.
	LoadOrStore(*entity.Host) (*entity.Host, bool)

	// Delete deletes host entity for a key
	Delete(string)
}

type host struct {
	// Host sync map
	*sync.Map
}

func newHost() Host {
	return &host{&sync.Map{}}
}

func (h *host) Load(key string) (*entity.Host, bool) {
	rawHost, ok := h.Map.Load(key)
	if !ok {
		return nil, false
	}

	return rawHost.(*entity.Host), ok
}

func (h *host) Store(host *entity.Host) {
	h.Map.Store(host.ID, host)
}

func (h *host) LoadOrStore(host *entity.Host) (*entity.Host, bool) {
	rawHost, loaded := h.Map.LoadOrStore(host.ID, host)
	return rawHost.(*entity.Host), loaded
}

func (h *host) Delete(key string) {
	h.Map.Delete(key)
}
