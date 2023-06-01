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

	logger "d7y.io/dragonfly/v2/internal/dflog"
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
	// Has to check if there is a connection between source host and destination host.
	Has(string, string) bool

	// Store stores source host and destination host.
	Store(string, string) error

	// DeleteHost deletes source host and all destination host connected to source host.
	DeleteHost(string) error

	// Probes loads probes interface by source host id and destination host id.
	Probes(string, string) Probes

	// ProbedCount is the number of times the host has been probed.
	ProbedCount(string) (uint64, error)

	// ProbedAt is the time when the host was last probed.
	ProbedAt(string) (time.Time, error)
}

// networkTopology is an implementation of network topology.
type networkTopology struct {
	// config is the network topology config.
	config config.NetworkTopologyConfig

	// rdb is Redis universal client interface.
	rdb redis.UniversalClient

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

// Has to check if there is a connection between source host and destination host.
func (nt *networkTopology) Has(srcHostID string, destHostID string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)
	defer cancel()

	networkTopologyCount, err := nt.rdb.Exists(ctx, pkgredis.MakeNetworkTopologyKeyInScheduler(srcHostID, destHostID)).Result()
	if err != nil {
		logger.Errorf("failed to check whether network topology exists: %s", err.Error())
		return false
	}

	return networkTopologyCount == 1
}

// Store stores source host and destination host.
func (nt *networkTopology) Store(srcHostID string, destHostID string) error {
	// If the network topology already exists, skip it.
	if nt.Has(srcHostID, destHostID) {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)
	defer cancel()

	if _, err := nt.rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.HSet(ctx, pkgredis.MakeNetworkTopologyKeyInScheduler(srcHostID, destHostID), "createdAt", time.Now().Format(time.RFC3339Nano))
		pipe.Set(ctx, pkgredis.MakeProbedCountKeyInScheduler(destHostID), 0, 0)
		return nil
	}); err != nil {
		return err
	}

	return nil
}

// DeleteHost deletes source host and all destination host connected to source host.
func (nt *networkTopology) DeleteHost(hostID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)
	defer cancel()

	if _, err := nt.rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Del(ctx, pkgredis.MakeNetworkTopologyKeyInScheduler(hostID, "*"))
		pipe.Del(ctx, pkgredis.MakeNetworkTopologyKeyInScheduler("*", hostID))
		pipe.Del(ctx, pkgredis.MakeProbesKeyInScheduler(hostID, "*"))
		pipe.Del(ctx, pkgredis.MakeProbesKeyInScheduler("*", hostID))
		pipe.Del(ctx, pkgredis.MakeProbedAtKeyInScheduler(hostID))
		pipe.Del(ctx, pkgredis.MakeProbedCountKeyInScheduler(hostID))
		return nil
	}); err != nil {
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

// ProbedAt is the time of the last probe.
func (nt *networkTopology) ProbedAt(hostID string) (time.Time, error) {
	ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)
	defer cancel()

	return nt.rdb.Get(ctx, pkgredis.MakeProbedAtKeyInScheduler(hostID)).Time()
}

// ProbedAt is the time when the host was last probed.
func (nt *networkTopology) Probes(srcHostID, destHostID string) Probes {
	return NewProbes(nt.config.Probe, nt.rdb, srcHostID, destHostID)
}
