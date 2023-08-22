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

	"d7y.io/dragonfly/v2/pkg/container/set"
	pkggc "d7y.io/dragonfly/v2/pkg/gc"
	"d7y.io/dragonfly/v2/pkg/types"
	"d7y.io/dragonfly/v2/scheduler/config"
)

const (
	// GC host id.
	GCHostID = "host"
)

// HostManager is the interface used for host manager.
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

	// Range calls f sequentially for each key and value present in the map.
	// If f returns false, range stops the iteration.
	Range(f func(any, any) bool)

	// LoadRandomHosts loads host randomly through the Range of sync.Map.
	LoadRandomHosts(int, set.SafeSet[string]) []*Host

	// Try to reclaim host.
	RunGC() error
}

// hostManager contains content for host manager.
type hostManager struct {
	// Host sync map.
	*sync.Map
}

// New host manager interface.
func newHostManager(cfg *config.GCConfig, gc pkggc.GC) (HostManager, error) {
	h := &hostManager{
		Map: &sync.Map{},
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

// Load returns host for a key.
func (h *hostManager) Load(key string) (*Host, bool) {
	rawHost, loaded := h.Map.Load(key)
	if !loaded {
		return nil, false
	}

	return rawHost.(*Host), loaded
}

// Store sets host.
func (h *hostManager) Store(host *Host) {
	h.Map.Store(host.ID, host)
}

// LoadOrStore returns host the key if present.
// Otherwise, it stores and returns the given host.
// The loaded result is true if the host was loaded, false if stored.
func (h *hostManager) LoadOrStore(host *Host) (*Host, bool) {
	rawHost, loaded := h.Map.LoadOrStore(host.ID, host)
	return rawHost.(*Host), loaded
}

// Delete deletes host for a key.
func (h *hostManager) Delete(key string) {
	h.Map.Delete(key)
}

// Range calls f sequentially for each key and value present in the map.
// If f returns false, range stops the iteration.
func (h *hostManager) Range(f func(key, value any) bool) {
	h.Map.Range(f)
}

// LoadRandomHosts loads host randomly through the Range of sync.Map.
func (h *hostManager) LoadRandomHosts(n int, blocklist set.SafeSet[string]) []*Host {
	hosts := make([]*Host, 0, n)
	h.Map.Range(func(key, value any) bool {
		if len(hosts) >= n {
			return false
		}

		host, ok := value.(*Host)
		if !ok {
			host.Log.Error("invalid host")
			return true
		}

		if blocklist.Contains(host.ID) {
			return true
		}

		hosts = append(hosts, host)
		return true
	})

	return hosts
}

// Try to reclaim host.
func (h *hostManager) RunGC() error {
	h.Map.Range(func(_, value any) bool {
		host, ok := value.(*Host)
		if !ok {
			host.Log.Error("invalid host")
			return true
		}

		if host.PeerCount.Load() == 0 &&
			host.ConcurrentUploadCount.Load() == 0 &&
			host.Type == types.HostTypeNormal {
			host.Log.Info("host has been reclaimed")
			h.Delete(host.ID)
		}

		return true
	})

	return nil
}
