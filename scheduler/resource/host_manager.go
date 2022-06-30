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

//go:generate mockgen -destination host_manager_mock.go -source host_manager.go -package resource

package resource

import (
	"sync"
	"time"

	pkggc "d7y.io/dragonfly/v2/pkg/gc"
	"d7y.io/dragonfly/v2/scheduler/config"
)

const (
	// GC host id.
	GCHostID = "host"
)

type HostManager interface {
	// Load returns host for a key.
	Load(string) (*Host, bool)

	// Store sets host.
	Store(*Host)

	// LoadOrStore returns host the key if present.
	// Otherwise, it stores and returns the given host.
	// The loaded result is true if the host was loaded, false if stored.
	LoadOrStore(*Host) (*Host, bool)

	// Delete deletes host for a key.
	Delete(string)

	// Try to reclaim host.
	RunGC() error
}

type hostManager struct {
	// Host sync map.
	*sync.Map

	// Host time to live.
	ttl time.Duration
}

// New host manager interface.
func newHostManager(cfg *config.GCConfig, gc pkggc.GC) (HostManager, error) {
	h := &hostManager{
		Map: &sync.Map{},
		ttl: cfg.HostTTL,
	}

	if err := gc.Add(pkggc.Task{
		ID:       GCHostID,
		Interval: cfg.HostGCInterval,
		Timeout:  cfg.HostGCInterval,
		Runner:   h,
	}); err != nil {
		return nil, err
	}

	return h, nil
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

func (h *hostManager) RunGC() error {
	h.Map.Range(func(_, value any) bool {
		host := value.(*Host)
		elapsed := time.Since(host.UpdateAt.Load())

		if elapsed > h.ttl &&
			host.PeerCount.Load() == 0 &&
			host.UploadPeerCount.Load() == 0 &&
			host.Type == HostTypeNormal {
			host.Log.Info("host has been reclaimed")
			h.Delete(host.ID)
		}

		return true
	})

	return nil
}
