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
	// LoadDestHosts returns parents for a key.
	LoadDestHosts(key string) ([]string, error)

	// StoreDestHosts stores parents.
	StoreDestHosts(key string, destHosts []string)

	// DeleteDestHosts deletes parents for a key.
	DeleteDestHosts(key string)

	// LoadProbes returns probes between two hosts.
	LoadProbes(src, dest string) (Probes, error)

	// StoreProbes stores probes between two hosts.
	StoreProbes(src, dest string, probes Probes) error

	// DeleteProbes deletes probes between two hosts.
	DeleteProbes(src, dest string) error

	// StoreProbe stores probe between two hosts.
	StoreProbe(src, dest string, probe Probe) error
}

type networkTopology struct {
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

	// mu locks for network topology.
	mu *sync.RWMutex
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
		rdb:           rdb,
		config:        cfg,
		resource:      resource,
		storage:       storage,
		managerClient: managerClient,
	}, nil
}

// LoadDestHosts returns parents for a key.
func (n *networkTopology) LoadDestHosts(key string) ([]string, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return nil, nil
}

// StoreDestHosts stores parents.
func (n *networkTopology) StoreDestHosts(key string, dest []string) {
	n.mu.Lock()
	defer n.mu.Unlock()
}

// DeleteDestHosts deletes parents for a key.
func (n *networkTopology) DeleteDestHosts(key string) {
	n.mu.Lock()
	defer n.mu.Unlock()
}

// LoadProbes returns probes between two hosts.
func (n *networkTopology) LoadProbes(src, dest string) (Probes, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return nil, nil
}

// StoreProbes stores probes between two hosts.
func (n *networkTopology) StoreProbes(src, dest string, probes Probes) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	return nil
}

// DeleteProbes deletes probes between two hosts.
func (n *networkTopology) DeleteProbes(src, dest string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	return nil
}

// StoreProbe stores probe between two hosts.
func (n *networkTopology) StoreProbe(src, dest string, probe Probe) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	return nil
}
