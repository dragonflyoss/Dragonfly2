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
	"errors"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/cache"
	"d7y.io/dragonfly/v2/pkg/container/set"
	pkgredis "d7y.io/dragonfly/v2/pkg/redis"
	"d7y.io/dragonfly/v2/pkg/slices"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/resource"
	"d7y.io/dragonfly/v2/scheduler/storage"
)

const (
	// contextTimeout is the timeout of redis invoke.
	contextTimeout = 2 * time.Minute

	// snapshotContextTimeout is the timeout of snapshot network topology.
	snapshotContextTimeout = 20 * time.Minute

	// findProbedCandidateHostsLimit is the limit of find probed candidate hosts.
	findProbedCandidateHostsLimit = 50
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

	// FindProbedHosts finds the most candidate destination host to be probed, randomly find a range of hosts,
	// and then return the host with a smaller probed count.
	FindProbedHosts(string) ([]*resource.Host, error)

	// DeleteHost deletes source host and all destination host connected to source host.
	DeleteHost(string) error

	// Probes loads probes interface by source host id and destination host id.
	Probes(string, string) Probes

	// ProbedCount is the number of times the host has been probed.
	ProbedCount(string) (uint64, error)

	// Snapshot writes the current network topology to the storage.
	Snapshot() error
}

// networkTopology is an implementation of network topology.
type networkTopology struct {
	// config is the network topology config.
	config config.NetworkTopologyConfig

	// rdb is Redis universal client interface.
	rdb redis.UniversalClient

	// Cache instance.
	cache cache.Cache

	// resource is resource interface.
	resource resource.Resource

	// storage is storage interface.
	storage storage.Storage

	// done channel will be closed when network topology serve stop.
	done chan struct{}
}

// New network topology interface.
func NewNetworkTopology(cfg config.NetworkTopologyConfig, rdb redis.UniversalClient, cache cache.Cache, resource resource.Resource, storage storage.Storage) (NetworkTopology, error) {
	return &networkTopology{
		config:   cfg,
		rdb:      rdb,
		cache:    cache,
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

	networkTopologyKey := pkgredis.MakeNetworkTopologyKeyInScheduler(srcHostID, destHostID)
	if _, _, ok := nt.cache.GetWithExpiration(networkTopologyKey); ok {
		return true
	}

	networkTopology, err := nt.rdb.HGetAll(ctx, networkTopologyKey).Result()
	if err != nil {
		logger.Errorf("get networkTopology failed: %s", err.Error())
		return false
	}

	if len(networkTopology) == 0 {
		return false
	}

	// Add cache data.
	nt.cache.Set(networkTopologyKey, networkTopology, nt.config.Cache.TTL)

	return true
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

	return nil
}

// FindProbedHosts finds the most candidate destination host to be probed, randomly find a range of hosts,
// and then return the host with a smaller probed count.
func (nt *networkTopology) FindProbedHosts(hostID string) ([]*resource.Host, error) {
	ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)
	defer cancel()

	blocklist := set.NewSafeSet[string]()
	blocklist.Add(hostID)
	hosts := nt.resource.HostManager().LoadRandomHosts(findProbedCandidateHostsLimit, blocklist)
	if len(hosts) == 0 {
		return nil, errors.New("probed hosts not found")
	}

	if len(hosts) <= nt.config.Probe.Count {
		return hosts, nil
	}

	var (
		probedCounts     []uint64
		probedCountKeys  []string
		probedCountHosts []*resource.Host
	)
	for _, host := range hosts {
		probedCountKey := pkgredis.MakeProbedCountKeyInScheduler(host.ID)
		cache, _, ok := nt.cache.GetWithExpiration(probedCountKey)
		if !ok {
			probedCountHosts = append(probedCountHosts, host)
			probedCountKeys = append(probedCountKeys, probedCountKey)
			continue
		} else {
			probedCount, ok := cache.(uint64)
			if ok {
				probedCounts = append(probedCounts, probedCount)
			} else {
				probedCounts = append(probedCounts, uint64(0))
			}
		}
	}
	candidateHosts, _ := slices.Difference(hosts, probedCountHosts)

	rawProbedCounts, err := nt.rdb.MGet(ctx, probedCountKeys...).Result()
	if err != nil {
		return nil, err
	}

	// Filter invalid probed count. If probed key not exist, the probed count is nil.
	for i, rawProbedCount := range rawProbedCounts {
		// Initialize the probedCount value of host in redis when the host is first selected as the candidate probe target.
		if rawProbedCount == nil {
			if err := nt.rdb.Set(ctx, probedCountKeys[i], 0, 0).Err(); err != nil {
				return nil, err
			}

			probedCounts = append(probedCounts, 0)
			continue
		}

		value, ok := rawProbedCount.(string)
		if !ok {
			return nil, errors.New("invalid value type")
		}

		probedCount, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return nil, errors.New("invalid probed count")
		}

		// Add cache data.
		nt.cache.Set(probedCountKeys[i], probedCount, nt.config.Cache.TTL)

		probedCounts = append(probedCounts, probedCount)
	}
	candidateHosts = append(candidateHosts, probedCountHosts...)

	// Sort candidate hosts by probed count.
	sort.Slice(candidateHosts, func(i, j int) bool {
		return probedCounts[i] < probedCounts[j]
	})

	return candidateHosts[:nt.config.Probe.Count], nil
}

// DeleteHost deletes source host and all destination host connected to source host.
func (nt *networkTopology) DeleteHost(hostID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)
	defer cancel()

	deleteKeys := []string{pkgredis.MakeProbedCountKeyInScheduler(hostID)}
	srcNetworkTopologyKeys, _, err := nt.rdb.Scan(ctx, 0, pkgredis.MakeNetworkTopologyKeyInScheduler(hostID, "*"), math.MaxInt64).Result()
	if err != nil {
		return err
	}
	deleteKeys = append(deleteKeys, srcNetworkTopologyKeys...)

	destNetworkTopologyKeys, _, err := nt.rdb.Scan(ctx, 0, pkgredis.MakeNetworkTopologyKeyInScheduler("*", hostID), math.MaxInt64).Result()
	if err != nil {
		return err
	}
	deleteKeys = append(deleteKeys, destNetworkTopologyKeys...)

	srcProbesKeys, _, err := nt.rdb.Scan(ctx, 0, pkgredis.MakeProbesKeyInScheduler(hostID, "*"), math.MaxInt64).Result()
	if err != nil {
		return err
	}
	deleteKeys = append(deleteKeys, srcProbesKeys...)

	destProbesKeys, _, err := nt.rdb.Scan(ctx, 0, pkgredis.MakeProbesKeyInScheduler("*", hostID), math.MaxInt64).Result()
	if err != nil {
		return err
	}
	deleteKeys = append(deleteKeys, destProbesKeys...)

	if err := nt.rdb.Del(ctx, deleteKeys...).Err(); err != nil {
		return err
	}

	for _, deleteKey := range deleteKeys {
		nt.cache.Delete(deleteKey)
	}

	return nil
}

// Probes loads probes interface by source host id and destination host id.
func (nt *networkTopology) Probes(srcHostID, destHostID string) Probes {
	return NewProbes(nt.config, nt.rdb, nt.cache, srcHostID, destHostID)
}

// ProbedCount is the number of times the host has been probed.
func (nt *networkTopology) ProbedCount(hostID string) (uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)
	defer cancel()

	probedCountKey := pkgredis.MakeProbedCountKeyInScheduler(hostID)
	if cache, _, ok := nt.cache.GetWithExpiration(probedCountKey); ok {
		probedCount, ok := cache.(uint64)
		if ok {
			return probedCount, nil
		}

		return uint64(0), errors.New("get probedCount failed")
	}

	probedCount, err := nt.rdb.Get(ctx, probedCountKey).Uint64()
	if err != nil {
		return uint64(0), err
	}

	// Add cache data.
	nt.cache.Set(probedCountKey, probedCount, nt.config.Cache.TTL)

	return probedCount, nil
}

// Snapshot writes the current network topology to the storage.
func (nt *networkTopology) Snapshot() error {
	ctx, cancel := context.WithTimeout(context.Background(), snapshotContextTimeout)
	defer cancel()

	now := time.Now()
	id := uuid.NewString()
	probedCountKeys, _, err := nt.rdb.Scan(ctx, 0, pkgredis.MakeProbedCountKeyInScheduler("*"), math.MaxInt64).Result()
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
		networkTopologyKeys, _, err := nt.rdb.Scan(ctx, 0, pkgredis.MakeNetworkTopologyKeyInScheduler(srcHostID, "*"), math.MaxInt64).Result()
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
			ID: id,
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
