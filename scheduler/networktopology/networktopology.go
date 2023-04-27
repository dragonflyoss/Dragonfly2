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

//go:generate mockgen -destination mocks/networktopology_mock.go -source networktopology.go -package mocks

package networktopology

import (
	"sync"

	"github.com/go-redis/redis/v8"

	pkgredis "d7y.io/dragonfly/v2/pkg/redis"
	managerclient "d7y.io/dragonfly/v2/pkg/rpc/manager/client"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/resource"
	"d7y.io/dragonfly/v2/scheduler/storage"
)

type NetworkTopology interface {
	// LoadParents returns parents for a key.
	LoadParents(string) (*sync.Map, bool)

	// StoreParents stores parents.
	StoreParents(key string, parents *sync.Map)

	// DeleteParents deletes parents for a key.
	DeleteParents(string)

	// LoadProbes returns probes between two hosts.
	LoadProbes(src, dest string) (Probes, bool)

	// StoreProbes stores probes between two hosts.
	StoreProbes(src, dest string, probes Probes) bool

	// DeleteProbes deletes probes between two hosts.
	DeleteProbes(src, dest string) bool
}

type networkTopology struct {
	// network topology
	*sync.Map

	// Redis universal client interface.
	rdb redis.UniversalClient

	// Scheduler config.
	config *config.Config

	// Resource interface
	resource resource.Resource

	// Storage interface
	storage storage.Storage

	// Manager client interface
	managerClient managerclient.V2
}

// New network topology interface.
func NewNetworkTopology(cfg *config.Config, resource resource.Resource, storage storage.Storage, managerClient managerclient.V2) (NetworkTopology, error) {

	rdb, err := pkgredis.NewRedis(&redis.UniversalOptions{
		Addrs:      cfg.Database.Redis.Addrs,
		MasterName: cfg.Database.Redis.MasterName,
		DB:         cfg.Database.Redis.NetworkTopologyDB,
		Username:   cfg.Database.Redis.Username,
		Password:   cfg.Database.Redis.Password,
	})
	if err != nil {
		return nil, err
	}

	return &networkTopology{
		Map:           &sync.Map{},
		rdb:           rdb,
		config:        cfg,
		resource:      resource,
		storage:       storage,
		managerClient: managerClient,
	}, nil
}

// LoadParents returns parents for a key.
func (n *networkTopology) LoadParents(key string) (*sync.Map, bool) {
	value, loaded := n.Map.Load(key)
	if !loaded {
		return nil, false
	}

	parents, ok := value.(*sync.Map)
	if !ok {
		return nil, false
	}

	return parents, true
}

// StoreParents stores parents.
func (n *networkTopology) StoreParents(key string, parents *sync.Map) {
	n.Map.Store(key, parents)
}

// DeleteParents deletes parents for a key.
func (n *networkTopology) DeleteParents(key string) {
	n.Map.Delete(key)
}

// LoadProbes returns probes between two hosts.
func (n *networkTopology) LoadProbes(src, dest string) (Probes, bool) {
	value, loaded := n.Map.Load(src)
	if !loaded {
		return nil, false
	}

	parents, ok := value.(*sync.Map)
	if !ok {
		return nil, false
	}

	p, loaded := parents.Load(dest)
	if !loaded {
		return nil, false
	}

	probes, ok := p.(*probes)
	if !ok {
		return nil, false
	}

	return probes, true
}

// StoreProbes stores probes between two hosts.
func (n *networkTopology) StoreProbes(src, dest string, probes Probes) bool {
	value, loaded := n.Map.Load(src)
	if !loaded {
		return false
	}

	parents, ok := value.(*sync.Map)
	if !ok {
		return false
	}

	parents.Store(dest, probes)
	return true
}

// DeleteProbes deletes probes between two hosts.
func (n *networkTopology) DeleteProbes(src, dest string) bool {
	value, loaded := n.Map.Load(src)
	if !loaded {
		return false
	}

	parents, ok := value.(*sync.Map)
	if !ok {
		return false
	}

	parents.Delete(dest)
	return true
}
