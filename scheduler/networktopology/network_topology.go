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
	"github.com/google/uuid"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	pkgredis "d7y.io/dragonfly/v2/pkg/redis"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/resource"
	"d7y.io/dragonfly/v2/scheduler/storage"
)

const (
	// contextTimeout is the timeout of redis invoke.
	contextTimeout = 2 * time.Minute

	// snapshotContextTimeout is the timeout of snapshot network topology.
	snapshotContextTimeout = 20 * time.Minute
)

// NetworkTopology is an interface for network topology.
type NetworkTopology interface {
	// Started network topology server.
	Serve()

	// Stop network topology server.
	Stop()

	// Has to check if there is a connection between source host and destination host.
	Has(string, string) bool

	// Store stores source host and destination host.
	Store(string, string) error

	// TODO Implement function.
	// FindProbedHostIDs finds the most candidate destination host to be probed.
	FindProbedHostIDs(string) ([]string, error)

	// DeleteHost deletes source host and all destination host connected to source host.
	DeleteHost(string) error

	// Probes loads probes interface by source host id and destination host id.
	Probes(string, string) Probes

	// ProbedCount is the number of times the host has been probed.
	ProbedCount(string) (uint64, error)

	// ProbedAt is the time when the host was last probed.
	ProbedAt(string) (time.Time, error)

	// Snapshot writes the current network topology to the storage.
	Snapshot() error
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

	// done channel will be closed when network topology serve stop.
	done chan struct{}
}

// New network topology interface.
func NewNetworkTopology(cfg config.NetworkTopologyConfig, rdb redis.UniversalClient, resource resource.Resource, storage storage.Storage) (NetworkTopology, error) {
	return &networkTopology{
		config:   cfg,
		rdb:      rdb,
		resource: resource,
		storage:  storage,
		done:     make(chan struct{}),
	}, nil
}

// Started network topology server.
func (nt *networkTopology) Serve() {
	logger.Info("collect network topology records")
	tick := time.NewTicker(nt.config.CollectInterval)
	for {
		select {
		case <-tick.C:
			if err := nt.Snapshot(); err != nil {
				logger.Error(err)
				break
			}
		case <-nt.done:
			return
		}
	}
}

// Stop network topology server.
func (nt *networkTopology) Stop() {
	close(nt.done)
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

	if err := nt.rdb.HSet(ctx, pkgredis.MakeNetworkTopologyKeyInScheduler(srcHostID, destHostID), "createdAt", time.Now().Format(time.RFC3339Nano)).Err(); err != nil {
		return err
	}

	if err := nt.rdb.Set(ctx, pkgredis.MakeProbedCountKeyInScheduler(destHostID), 0, 0).Err(); err != nil {
		return err
	}

	return nil
}

// TODO Implement function.
// FindProbedHostIDs finds the most candidate destination host to be probed.
func (nt *networkTopology) FindProbedHostIDs(hostID string) ([]string, error) {
	return nil, nil
}

// DeleteHost deletes source host and all destination host connected to source host.
func (nt *networkTopology) DeleteHost(hostID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)
	defer cancel()

	deleteKeys := []string{pkgredis.MakeProbedAtKeyInScheduler(hostID), pkgredis.MakeProbedCountKeyInScheduler(hostID)}
	srcNetworkTopologyKeys, err := nt.rdb.Keys(ctx, pkgredis.MakeNetworkTopologyKeyInScheduler(hostID, "*")).Result()
	if err != nil {
		return err
	}
	deleteKeys = append(deleteKeys, srcNetworkTopologyKeys...)

	destNetworkTopologyKeys, err := nt.rdb.Keys(ctx, pkgredis.MakeNetworkTopologyKeyInScheduler("*", hostID)).Result()
	if err != nil {
		return err
	}
	deleteKeys = append(deleteKeys, destNetworkTopologyKeys...)

	srcProbesKeys, err := nt.rdb.Keys(ctx, pkgredis.MakeProbesKeyInScheduler(hostID, "*")).Result()
	if err != nil {
		return err
	}
	deleteKeys = append(deleteKeys, srcProbesKeys...)

	destProbesKeys, err := nt.rdb.Keys(ctx, pkgredis.MakeProbesKeyInScheduler("*", hostID)).Result()
	if err != nil {
		return err
	}
	deleteKeys = append(deleteKeys, destProbesKeys...)

	if err := nt.rdb.Del(ctx, deleteKeys...).Err(); err != nil {
		return err
	}

	return nil
}

// Probes loads probes interface by source host id and destination host id.
func (nt *networkTopology) Probes(srcHostID, destHostID string) Probes {
	return NewProbes(nt.config.Probe, nt.rdb, srcHostID, destHostID)
}

// ProbedCount is the number of times the host has been probed.
func (nt *networkTopology) ProbedCount(hostID string) (uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)
	defer cancel()

	return nt.rdb.Get(ctx, pkgredis.MakeProbedCountKeyInScheduler(hostID)).Uint64()
}

// ProbedAt is the time when the host was last probed.
func (nt *networkTopology) ProbedAt(hostID string) (time.Time, error) {
	ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)
	defer cancel()

	return nt.rdb.Get(ctx, pkgredis.MakeProbedAtKeyInScheduler(hostID)).Time()
}

// Snapshot writes the current network topology to the storage.
func (nt *networkTopology) Snapshot() error {
	ctx, cancel := context.WithTimeout(context.Background(), snapshotContextTimeout)
	defer cancel()

	now := time.Now()
	probedCountKeys, err := nt.rdb.Keys(ctx, pkgredis.MakeProbedCountKeyInScheduler("*")).Result()
	if err != nil {
		return err
	}

	for _, probedCountKey := range probedCountKeys {
		_, _, srcHostID, err := pkgredis.ParseProbedCountKeyInScheduler(probedCountKey)
		if err != nil {
			logger.Error(err)
			continue
		}

		// Construct destination hosts for network topology.
		networkTopologyKeys, err := nt.rdb.Keys(ctx, pkgredis.MakeNetworkTopologyKeyInScheduler(srcHostID, "*")).Result()
		if err != nil {
			logger.Error(err)
			continue
		}

		destHosts := make([]storage.DestHost, 0, len(networkTopologyKeys))
		for _, networkTopologyKey := range networkTopologyKeys {
			_, _, srcHostID, destHostID, err := pkgredis.ParseNetworkTopologyKeyInScheduler(networkTopologyKey)
			if err != nil {
				logger.Error(err)
				continue
			}

			host, loaded := nt.resource.HostManager().Load(destHostID)
			if !loaded {
				logger.Errorf("host %s not found", destHostID)
				continue
			}

			ps := nt.Probes(srcHostID, destHostID)
			averageRTT, err := ps.AverageRTT()
			if err != nil {
				logger.Error(err)
				continue
			}

			createdAt, err := ps.CreatedAt()
			if err != nil {
				logger.Error(err)
				continue
			}

			updatedAt, err := ps.UpdatedAt()
			if err != nil {
				logger.Error(err)
				continue
			}

			destHost := storage.DestHost{
				ID:       host.ID,
				Type:     host.Type.Name(),
				Hostname: host.Hostname,
				IP:       host.IP,
				Port:     host.Port,
				Network: resource.Network{
					TCPConnectionCount:       host.Network.TCPConnectionCount,
					UploadTCPConnectionCount: host.Network.UploadTCPConnectionCount,
					Location:                 host.Network.Location,
					IDC:                      host.Network.IDC,
				},
				Probes: storage.Probes{
					AverageRTT: averageRTT.Nanoseconds(),
					CreatedAt:  createdAt.UnixNano(),
					UpdatedAt:  updatedAt.UnixNano(),
				},
			}

			destHosts = append(destHosts, destHost)
		}

		// Construct source hosts for network topology.
		host, loaded := nt.resource.HostManager().Load(srcHostID)
		if !loaded {
			logger.Errorf("host %s not found", srcHostID)
			continue
		}

		if err = nt.storage.CreateNetworkTopology(storage.NetworkTopology{
			ID: uuid.NewString(),
			Host: storage.SrcHost{
				ID:       host.ID,
				Type:     host.Type.Name(),
				Hostname: host.Hostname,
				IP:       host.IP,
				Port:     host.Port,
				Network: resource.Network{
					TCPConnectionCount:       host.Network.TCPConnectionCount,
					UploadTCPConnectionCount: host.Network.UploadTCPConnectionCount,
					Location:                 host.Network.Location,
					IDC:                      host.Network.IDC,
				},
			},
			DestHosts: destHosts,
			CreatedAt: now.UnixNano(),
		}); err != nil {
			logger.Error(err)
			continue
		}
	}

	return nil
}
