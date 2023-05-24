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
	"time"

	"github.com/go-redis/redis/v8"

	pkgredis "d7y.io/dragonfly/v2/pkg/redis"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/resource"
	"d7y.io/dragonfly/v2/scheduler/storage"
)

const (
	// contextTimeout is the timeout of redis invoke.
	contextTimeout = 2 * time.Minute
)

// NetworkTopology is an interface for network topology.
type NetworkTopology interface {
	// LoadDestHostIDs loads destination host ids by source host id.
	LoadDestHostIDs(string) ([]string, error)

	// DeleteHost deletes host.
	DeleteHost(string) error

	// ProbedCount is the number of times the host has been probed.
	ProbedCount(string) (uint64, error)

	// LoadProbes loads probes by source host id and destination host id.
	LoadProbes(string, string) Probes
}

// networkTopology is an implementation of network topology.
type networkTopology struct {
	// rdb is Redis universal client interface.
	rdb redis.UniversalClient

	// config is the network topology config.
	config config.NetworkTopologyConfig

	// resource is resource interface.
	resource resource.Resource

	// storage is storage interface.
	storage storage.Storage
}

// New network topology interface.
func NewNetworkTopology(cfg config.NetworkTopologyConfig, rdb redis.UniversalClient, resource resource.Resource, storage storage.Storage) (NetworkTopology, error) {
	return &networkTopology{
		config:   cfg,
		rdb:      rdb,
		resource: resource,
		storage:  storage,
	}, nil
}

// LoadDestHostIDs loads destination host ids by source host id.
func (nt *networkTopology) LoadDestHostIDs(hostID string) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)
	defer cancel()

	return nt.rdb.Keys(ctx, pkgredis.MakeNetworkTopologyKeyInScheduler(hostID, "*")).Result()
}

// DeleteHost deletes host.
func (nt *networkTopology) DeleteHost(hostID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)
	defer cancel()

	if err := nt.rdb.Del(ctx, pkgredis.MakeNetworkTopologyKeyInScheduler(hostID, "*")).Err(); err != nil {
		return err
	}

	if err := nt.rdb.Del(ctx, pkgredis.MakeProbesKeyInScheduler(hostID, "*")).Err(); err != nil {
		return err
	}

	count, err := nt.rdb.Del(ctx, pkgredis.MakeProbesKeyInScheduler("*", hostID)).Result()
	if err != nil {
		return err
	}

	if err = nt.rdb.DecrBy(ctx, pkgredis.MakeProbedCountKeyInScheduler(hostID), count).Err(); err != nil {
		return err
	}

	return nil
}

// ProbedCount is the number of times the host has been probed.
func (nt *networkTopology) ProbedCount(hostID string) (uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)
	defer cancel()

	return nt.rdb.Get(ctx, pkgredis.MakeProbedCountKeyInScheduler(hostID)).Uint64()
}

// LoadProbes loads probes by source host id and destination host id.
func (nt *networkTopology) LoadProbes(srcHostID, destHostID string) Probes {
	return NewProbes(nt.config.Probe, nt.rdb, srcHostID, destHostID)
}
