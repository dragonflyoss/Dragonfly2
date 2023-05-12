/*
 *     Copyright 2023 The Dragonfly Authors
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

package networktopology

import (
	"github.com/go-redis/redis/v8"

	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/resource"
	"d7y.io/dragonfly/v2/scheduler/storage"
)

// TODO(fcgxz2003): implement the network topology.
type NetworkTopology interface {
	// Peek returns the oldest probe without removing it.
	Peek(src, dest string) (*Probe, bool)
}

type networkTopology struct {
	// Scheduler config.
	config *config.Config

	// Redis universal client interface.
	rdb redis.UniversalClient

	// Resource interface.
	resource resource.Resource

	// Storage interface.
	storage storage.Storage
}

// New network topology interface.
func NewNetworkTopology(cfg *config.Config, rdb redis.UniversalClient, resource resource.Resource, storage storage.Storage) (NetworkTopology, error) {
	return &networkTopology{
		config:   cfg,
		rdb:      rdb,
		resource: resource,
		storage:  storage,
	}, nil
}

// Peek returns the oldest probe without removing it.
func (n *networkTopology) Peek(src, dest string) (*Probe, bool) {
	return nil, false
}
