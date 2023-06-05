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
	"strings"
	"time"

	"github.com/go-redis/redis/v8"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	pkgredis "d7y.io/dragonfly/v2/pkg/redis"
	"d7y.io/dragonfly/v2/pkg/slices"

	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/resource"
	"d7y.io/dragonfly/v2/scheduler/storage"
)

const (
	// contextTimeout is the timeout of redis invoke.
	contextTimeout = 2 * time.Minute

	// srcHostIDIndex is the source host id index of the network topology key.
	srcHostIDIndex = 2

	// destHostIDIndex is the destination host id index of the network topology key.
	destHostIDIndex = 3
)

// NetworkTopology is an interface for network topology.
type NetworkTopology interface {
	// Started network topology server.
	Serve() error

	// Stop network topology server.
	Stop() error

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
func (nt *networkTopology) Serve() error {
	logger.Info("store network topology records")
	if err := nt.createNetworkTopologyRecords(); err != nil {
		return err
	}

	return nil
}

// Stop network topology server.
func (nt *networkTopology) Stop() error {
	close(nt.done)
	return nil
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

// Probes is the interface to store probes.
func (nt *networkTopology) Probes(srcHostID, destHostID string) Probes {
	return NewProbes(nt.config.Probe, nt.rdb, srcHostID, destHostID)
}

// createNetworkTopologyRecords stores network topology records.
func (nt *networkTopology) createNetworkTopologyRecords() error {
	tick := time.NewTicker(nt.config.CollectInterval)
	for {
		select {
		case <-tick.C:
			if err := nt.create(time.Now().String()); err != nil {
				logger.Error(err)
			}
		case <-nt.done:
			return nil
		}
	}
}

// create loads network topology from redis and inserts it into csv file.
func (nt *networkTopology) create(networkTopologyID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)
	defer cancel()

	srcKeys, err := nt.rdb.Keys(ctx, pkgredis.MakeKeyInScheduler(pkgredis.NetworkTopologyNamespace, "*")).Result()
	if err != nil {
		return err
	}

	srcHostIDsWithoutDuplicates := make([]string, 0)
	for _, srcKey := range srcKeys {
		srcHostID := strings.Split(srcKey, ":")[srcHostIDIndex]
		if slices.Contains(srcHostIDsWithoutDuplicates, srcHostID) {
			continue
		}

		srcHostIDsWithoutDuplicates = append(srcHostIDsWithoutDuplicates, srcHostID)
		destKeys, err := nt.rdb.Keys(ctx, pkgredis.MakeNetworkTopologyKeyInScheduler(srcHostID, "*")).Result()
		if err != nil {
			return err
		}

		destHosts := make([]storage.DestHost, 0)
		for _, destKey := range destKeys {
			destHostID := strings.Split(destKey, ":")[destHostIDIndex]
			p := nt.Probes(srcHostID, destHostID)
			averageRTT, err := p.AverageRTT()
			if err != nil {
				return err
			}

			createdAt, err := p.CreatedAt()
			if err != nil {
				return err
			}

			updatedAt, err := p.UpdatedAt()
			if err != nil {
				return err
			}

			probes := storage.Probes{
				AverageRTT: averageRTT.Nanoseconds(),
				CreatedAt:  createdAt.UnixNano(),
				UpdatedAt:  updatedAt.UnixNano(),
			}

			dest, ok := nt.resource.HostManager().Load(destHostID)
			if !ok {
				return errors.New(destHostID + " does not exist")
			}

			destHost := storage.DestHost{
				Host: storage.Host{
					ID:                    dest.ID,
					Type:                  dest.Type.Name(),
					Hostname:              dest.Hostname,
					IP:                    dest.IP,
					Port:                  dest.Port,
					DownloadPort:          dest.DownloadPort,
					OS:                    dest.OS,
					Platform:              dest.Platform,
					PlatformFamily:        dest.PlatformFamily,
					PlatformVersion:       dest.PlatformVersion,
					KernelVersion:         dest.KernelVersion,
					ConcurrentUploadLimit: dest.ConcurrentUploadLimit.Load(),
					ConcurrentUploadCount: dest.ConcurrentUploadCount.Load(),
					UploadCount:           dest.UploadCount.Load(),
					UploadFailedCount:     dest.UploadFailedCount.Load(),
					CPU: resource.CPU{
						LogicalCount:   dest.CPU.LogicalCount,
						PhysicalCount:  dest.CPU.PhysicalCount,
						Percent:        dest.CPU.Percent,
						ProcessPercent: dest.CPU.ProcessPercent,
						Times: resource.CPUTimes{
							User:      dest.CPU.Times.User,
							System:    dest.CPU.Times.System,
							Idle:      dest.CPU.Times.Idle,
							Nice:      dest.CPU.Times.Nice,
							Iowait:    dest.CPU.Times.Iowait,
							Irq:       dest.CPU.Times.Irq,
							Softirq:   dest.CPU.Times.Softirq,
							Steal:     dest.CPU.Times.Steal,
							Guest:     dest.CPU.Times.Guest,
							GuestNice: dest.CPU.Times.GuestNice,
						},
					},
					Memory: resource.Memory{
						Total:              dest.Memory.Total,
						Available:          dest.Memory.Available,
						Used:               dest.Memory.Used,
						UsedPercent:        dest.Memory.UsedPercent,
						ProcessUsedPercent: dest.Memory.ProcessUsedPercent,
						Free:               dest.Memory.Free,
					},
					Network: resource.Network{
						TCPConnectionCount:       dest.Network.TCPConnectionCount,
						UploadTCPConnectionCount: dest.Network.UploadTCPConnectionCount,
						Location:                 dest.Network.Location,
						IDC:                      dest.Network.IDC,
					},
					Disk: resource.Disk{
						Total:             dest.Disk.Total,
						Free:              dest.Disk.Free,
						Used:              dest.Disk.Used,
						UsedPercent:       dest.Disk.UsedPercent,
						InodesTotal:       dest.Disk.InodesTotal,
						InodesUsed:        dest.Disk.InodesUsed,
						InodesFree:        dest.Disk.InodesFree,
						InodesUsedPercent: dest.Disk.InodesUsedPercent,
					},
					Build: resource.Build{
						GitVersion: dest.Build.GitVersion,
						GitCommit:  dest.Build.GitCommit,
						GoVersion:  dest.Build.GoVersion,
						Platform:   dest.Build.Platform,
					},
					CreatedAt: int64(dest.CreatedAt.Load().Nanosecond()),
					UpdatedAt: int64(dest.UpdatedAt.Load().Nanosecond()),
				},
				Probes: probes,
			}

			destHosts = append(destHosts, destHost)
		}

		src, ok := nt.resource.HostManager().Load(srcHostID)
		if !ok {
			return errors.New(srcHostID + " does not exist")
		}

		networkTopology := storage.NetworkTopology{
			ID: networkTopologyID,
			Host: storage.Host{
				ID:                    src.ID,
				Type:                  src.Type.Name(),
				Hostname:              src.Hostname,
				IP:                    src.IP,
				Port:                  src.Port,
				DownloadPort:          src.DownloadPort,
				OS:                    src.OS,
				Platform:              src.Platform,
				PlatformFamily:        src.PlatformFamily,
				PlatformVersion:       src.PlatformVersion,
				KernelVersion:         src.KernelVersion,
				ConcurrentUploadLimit: src.ConcurrentUploadLimit.Load(),
				ConcurrentUploadCount: src.ConcurrentUploadCount.Load(),
				UploadCount:           src.UploadCount.Load(),
				UploadFailedCount:     src.UploadFailedCount.Load(),
				CPU: resource.CPU{
					LogicalCount:   src.CPU.LogicalCount,
					PhysicalCount:  src.CPU.PhysicalCount,
					Percent:        src.CPU.Percent,
					ProcessPercent: src.CPU.ProcessPercent,
					Times: resource.CPUTimes{
						User:      src.CPU.Times.User,
						System:    src.CPU.Times.System,
						Idle:      src.CPU.Times.Idle,
						Nice:      src.CPU.Times.Nice,
						Iowait:    src.CPU.Times.Iowait,
						Irq:       src.CPU.Times.Irq,
						Softirq:   src.CPU.Times.Softirq,
						Steal:     src.CPU.Times.Steal,
						Guest:     src.CPU.Times.Guest,
						GuestNice: src.CPU.Times.GuestNice,
					},
				},
				Memory: resource.Memory{
					Total:              src.Memory.Total,
					Available:          src.Memory.Available,
					Used:               src.Memory.Used,
					UsedPercent:        src.Memory.UsedPercent,
					ProcessUsedPercent: src.Memory.ProcessUsedPercent,
					Free:               src.Memory.Free,
				},
				Network: resource.Network{
					TCPConnectionCount:       src.Network.TCPConnectionCount,
					UploadTCPConnectionCount: src.Network.UploadTCPConnectionCount,
					Location:                 src.Network.Location,
					IDC:                      src.Network.IDC,
				},
				Disk: resource.Disk{
					Total:             src.Disk.Total,
					Free:              src.Disk.Free,
					Used:              src.Disk.Used,
					UsedPercent:       src.Disk.UsedPercent,
					InodesTotal:       src.Disk.InodesTotal,
					InodesUsed:        src.Disk.InodesUsed,
					InodesFree:        src.Disk.InodesFree,
					InodesUsedPercent: src.Disk.InodesUsedPercent,
				},
				Build: resource.Build{
					GitVersion: src.Build.GitVersion,
					GitCommit:  src.Build.GitCommit,
					GoVersion:  src.Build.GoVersion,
					Platform:   src.Build.Platform,
				},
				CreatedAt: int64(src.CreatedAt.Load().Nanosecond()),
				UpdatedAt: int64(src.UpdatedAt.Load().Nanosecond()),
			},
			DestHosts: destHosts,
		}

		if err = nt.storage.CreateNetworkTopology(networkTopology); err != nil {
			return err
		}
	}

	return nil
}
