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

//go:generate mockgen -destination mocks/network_topology_mock.go -source network_topology.go -package mocks

package networktopology

import (
	"context"
	"fmt"
	"strconv"

	"github.com/go-redis/redis/v8"

	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/resource"
	"d7y.io/dragonfly/v2/scheduler/storage"
)

type NetworkTopology interface {
	// ProbeCount is the number of times the host probes.
	ProbeCount(hostID string) int64

	// ProbedCount is the number of times the host has been probed.
	ProbedCount(hostID string) int64

	// LoadDestHosts returns destination hosts for source host.
	LoadDestHosts(hostID string) ([]string, bool)

	// DeleteHost deletes host.
	DeleteHost(hostID string) error

	// StoreProbe stores probe between two hosts.
	StoreProbe(src, dest string, probe *Probe) bool
}

type networkTopology struct {
	// Redis universal client interface.
	rdb redis.UniversalClient

	// Scheduler config.
	config *config.Config

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

// ProbeCount is the number of times the host probes.
func (n *networkTopology) ProbeCount(hostID string) int64 {
	key := fmt.Sprintf("probe-count:%s", hostID)
	value, err := n.rdb.Get(context.Background(), key).Result()
	if err != nil {
		return 0
	}

	probeCount, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0
	}

	return probeCount
}

// ProbedCount is the number of times the host has been probed.
func (n *networkTopology) ProbedCount(hostID string) int64 {
	key := fmt.Sprintf("probed-count:%s", hostID)
	value, err := n.rdb.Get(context.Background(), key).Result()
	if err != nil {
		return 0
	}

	probedCount, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0
	}

	return probedCount
}

// LoadDestHosts returns destination hosts for source host.
func (n *networkTopology) LoadDestHosts(hostID string) ([]string, bool) {
	key := fmt.Sprintf("network-topology:%s:*", hostID)
	keys, err := n.rdb.Keys(context.Background(), key).Result()
	if err != nil {
		return []string{}, false
	}

	destHosts := make([]string, 0)
	for _, k := range keys {
		destHosts = append(destHosts, k[len(key)-1:])
	}

	return destHosts, true
}

// DeleteHost deletes host.
func (n *networkTopology) DeleteHost(hostID string) error {
	// Delete network topology.
	key := fmt.Sprintf("network-topology:%s:*", hostID)
	if err := n.rdb.Del(context.Background(), key).Err(); err != nil {
		return err
	}

	// Delete probes sent by the host.
	key = fmt.Sprintf("probes:%s:*", hostID)
	if err := n.rdb.Del(context.Background(), key).Err(); err != nil {
		return err
	}

	// Delete probes sent to the host, and return the number of probes deleted for updating probed count.
	key = fmt.Sprintf("probes:*:%s", hostID)
	count, err := n.rdb.Del(context.Background(), key).Result()
	if err != nil {
		return err
	}

	// Delete probe count of host.
	key = fmt.Sprintf("probe-count:%s", hostID)
	if err = n.rdb.Del(context.Background(), key).Err(); err != nil {
		return err
	}

	// Decrease probed count of host.
	key = fmt.Sprintf("probed-count:%s", hostID)
	if err = n.rdb.DecrBy(context.Background(), key, count).Err(); err != nil {
		return err
	}

	return nil
}

// StoreProbe stores probe between two hosts.
func (n *networkTopology) StoreProbe(src, dest string, probe *Probe) bool {
	probes := NewProbes(n.rdb, n.config.NetworkTopology.Probe.QueueLength, src, dest)
	if err := probes.Enqueue(probe); err != nil {
		return false
	}

	// Update probe count.
	key := fmt.Sprintf("probe-count:%s", src)
	if err := n.rdb.Incr(context.Background(), key).Err(); err != nil {
		return false
	}

	// Update probed count.
	key = fmt.Sprintf("probed-count:%s", dest)
	if err := n.rdb.Incr(context.Background(), key).Err(); err != nil {
		return false
	}

	return true
}
