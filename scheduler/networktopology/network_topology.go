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
	// Has checks whether src host and destination host exist.
	Has(string, string) bool

	// Store stores src host and destination host.
	Store(string, string) error

	// Delete deletes src host and destination host. If destination host is *,
	// delete all destination hosts.
	Delete(string, string) error

	// Probes loads probes interface by source host id and destination host id.
	Probes(string, string) Probes

	// ProbedCount is the number of times the host has been probed.
	ProbedCount(string) (uint64, error)
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

// Has checks whether src host and destination host exist.
func (nt *networkTopology) Has(srcHostID string, destHostID string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)
	defer cancel()

	networkTopologyCount, err := nt.rdb.Exists(ctx, pkgredis.MakeNetworkTopologyKeyInScheduler(srcHostID, destHostID)).Result()
	if err != nil {
		logger.Errorf("failed to check whether network topology exists: %s", err.Error())
		return false
	}

	probesCount, err := nt.rdb.Exists(ctx, pkgredis.MakeProbesKeyInScheduler(srcHostID, destHostID)).Result()
	if err != nil {
		logger.Errorf("failed to check whether probes exists: %s", err.Error())
		return false
	}

	return networkTopologyCount == 1 && probesCount == 1
}

// Store stores src host and destination host.
func (nt *networkTopology) Store(srcHostID string, destHostID string) error {
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

// Delete deletes src host and destination host. If destination host is *,
// delete all destination hosts.
func (nt *networkTopology) Delete(srcHostID string, destHostID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)
	defer cancel()

	if _, err := nt.rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Del(ctx, pkgredis.MakeNetworkTopologyKeyInScheduler(srcHostID, destHostID))
		pipe.Del(ctx, pkgredis.MakeProbesKeyInScheduler(srcHostID, destHostID))
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

// Probes loads probes interface by source host id and destination host id.
func (nt *networkTopology) Probes(srcHostID, destHostID string) Probes {
	return NewProbes(nt.config.Probe, nt.rdb, srcHostID, destHostID)
}
