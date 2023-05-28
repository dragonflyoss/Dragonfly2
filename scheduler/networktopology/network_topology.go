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
	// Started network topology server.
	Serve() error

	// Stop network topology server.
	Stop() error

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

	// done is channel.
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
	if nt.config.Enable {
		logger.Info("announce scheduler to trainer")
		if err := nt.createNetworkTopology(); err != nil {
			return err
		}
	}

	return nil
}

// Stop network topology server.
func (nt *networkTopology) Stop() error {
	close(nt.done)
	return nil
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

// createNetworkTopology inserts network topology to csv file.
func (nt *networkTopology) createNetworkTopology() error {
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

// create loads the network topology from redis and inserts the network topology into csv file.
func (nt *networkTopology) create(id string) error {
	ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)
	defer cancel()

	keys, err := nt.rdb.Keys(ctx, pkgredis.MakeKeyInScheduler(pkgredis.NetworkTopologyNamespace, "*")).Result()
	if err != nil {
		return err
	}

	for _, key := range keys {
		words := strings.Split(key, ":")
		tmpSrcHostID := words[1]

		destKeys, err := nt.LoadDestHostIDs(tmpSrcHostID)
		if err != nil {
			return err
		}

		destHosts := make([]storage.DestHost, 0)
		for _, destKey := range destKeys {
			destWords := strings.Split(destKey, ":")
			tmpDestHostID := destWords[2]

			p := nt.LoadProbes(tmpSrcHostID, tmpDestHostID)
			averageRTT, err := p.AverageRTT()
			if err != nil {
				return err
			}

			updatedAt, err := p.UpdatedAt()
			if err != nil {
				return err
			}

			probes := storage.Probes{
				AverageRTT: averageRTT.Nanoseconds(),
				CreatedAt:  0,
				UpdatedAt:  updatedAt.UnixNano(),
			}

			dest, ok := nt.resource.HostManager().Load(tmpDestHostID)
			if !ok {
				return errors.New(tmpDestHostID + " does not exist.")
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

		src, ok := nt.resource.HostManager().Load(tmpSrcHostID)
		if !ok {
			return errors.New(tmpSrcHostID + " does not exist")
		}

		networkTopology := storage.NetworkTopology{
			ID: id,
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
