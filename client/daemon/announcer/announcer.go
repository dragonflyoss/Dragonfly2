/*
 *     Copyright 2022 The Dragonfly Authors
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

//go:generate mockgen -destination mocks/announcer_mock.go -source announcer.go -package mocks

package announcer

import (
	"context"
	"os"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/host"
	"github.com/shirou/gopsutil/v3/mem"
	gopsutilnet "github.com/shirou/gopsutil/v3/net"
	"github.com/shirou/gopsutil/v3/process"

	managerv1 "d7y.io/api/v2/pkg/apis/manager/v1"
	schedulerv1 "d7y.io/api/v2/pkg/apis/scheduler/v1"

	"d7y.io/dragonfly/v2/client/config"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	managerclient "d7y.io/dragonfly/v2/pkg/rpc/manager/client"
	schedulerclient "d7y.io/dragonfly/v2/pkg/rpc/scheduler/client"
	"d7y.io/dragonfly/v2/pkg/types"
	"d7y.io/dragonfly/v2/version"
)

// Announcer is the interface used for announce service.
type Announcer interface {
	// Started announcer server.
	Serve() error

	// Stop announcer server.
	Stop() error
}

// announcer provides announce function.
type announcer struct {
	config                  *config.DaemonOption
	dynconfig               config.Dynconfig
	hostID                  string
	daemonPort              int32
	daemonDownloadPort      int32
	daemonObjectStoragePort int32
	schedulerClient         schedulerclient.V1
	managerClient           managerclient.V1
	done                    chan struct{}
}

// Option is a functional option for configuring the announcer.
type Option func(s *announcer)

// WithManagerClient sets the grpc client of manager.
func WithManagerClient(client managerclient.V1) Option {
	return func(a *announcer) {
		a.managerClient = client
	}
}

// WithObjectStoragePort sets the daemonObjectStoragePort.
func WithObjectStoragePort(port int32) Option {
	return func(a *announcer) {
		a.daemonObjectStoragePort = port
	}
}

// New returns a new Announcer interface.
func New(cfg *config.DaemonOption, dynconfig config.Dynconfig, hostID string, daemonPort int32, daemonDownloadPort int32, schedulerClient schedulerclient.V1, options ...Option) Announcer {
	a := &announcer{
		config:             cfg,
		dynconfig:          dynconfig,
		hostID:             hostID,
		daemonPort:         daemonPort,
		daemonDownloadPort: daemonDownloadPort,
		schedulerClient:    schedulerClient,
		done:               make(chan struct{}),
	}

	for _, opt := range options {
		opt(a)
	}

	return a
}

// Started announcer server.
func (a *announcer) Serve() error {
	if a.managerClient != nil {
		logger.Info("announce seed peer to manager")
		if err := a.announceToManager(); err != nil {
			return err
		}
	}

	logger.Info("announce peer to scheduler")
	if err := a.announceToScheduler(); err != nil {
		return err
	}

	return nil
}

// Stop announcer server.
func (a *announcer) Stop() error {
	close(a.done)
	return nil
}

// announceToScheduler announces peer information to scheduler.
func (a *announcer) announceToScheduler() error {
	req, err := a.newAnnounceHostRequest()
	if err != nil {
		return err
	}

	if err := a.schedulerClient.AnnounceHost(context.Background(), req); err != nil {
		logger.Errorf("announce for the first time failed: %s", err.Error())
	}

	// Announce to scheduler.
	tick := time.NewTicker(a.config.Announcer.SchedulerInterval)
	for {
		select {
		case <-tick.C:
			req, err := a.newAnnounceHostRequest()
			if err != nil {
				logger.Error(err)
				break
			}

			if err := a.schedulerClient.AnnounceHost(context.Background(), req); err != nil {
				logger.Error(err)
				break
			}
		case <-a.done:
			return nil
		}
	}
}

// newAnnounceHostRequest returns announce host request.
func (a *announcer) newAnnounceHostRequest() (*schedulerv1.AnnounceHostRequest, error) {
	hostType := types.HostTypeNormalName
	if a.config.Scheduler.Manager.SeedPeer.Enable {
		hostType = types.HostTypeSuperSeedName
	}

	var objectStoragePort int32
	if a.config.ObjectStorage.Enable {
		objectStoragePort = a.daemonObjectStoragePort
	}

	pid := os.Getpid()

	h, err := host.Info()
	if err != nil {
		return nil, err
	}

	proc, err := process.NewProcess(int32(pid))
	if err != nil {
		return nil, err
	}

	procCPUPercent, err := proc.CPUPercent()
	if err != nil {
		return nil, err
	}

	cpuPercent, err := cpu.Percent(0, false)
	if err != nil {
		return nil, err
	}

	cpuLogicalCount, err := cpu.Counts(true)
	if err != nil {
		return nil, err
	}

	cpuPhysicalCount, err := cpu.Counts(false)
	if err != nil {
		return nil, err
	}

	cpuTimes, err := cpu.Times(false)
	if err != nil {
		return nil, err
	}

	procMemoryPercent, err := proc.MemoryPercent()
	if err != nil {
		return nil, err
	}

	virtualMemory, err := mem.VirtualMemory()
	if err != nil {
		return nil, err
	}

	procTCPConnections, err := gopsutilnet.ConnectionsPid("tcp", int32(pid))
	if err != nil {
		return nil, err
	}

	var uploadTCPConnections []gopsutilnet.ConnectionStat
	for _, procTCPConnection := range procTCPConnections {
		if procTCPConnection.Laddr.Port == uint32(a.daemonDownloadPort) && procTCPConnection.Status == "ESTABLISHED" {
			uploadTCPConnections = append(uploadTCPConnections, procTCPConnection)
		}
	}

	tcpConnections, err := gopsutilnet.Connections("tcp")
	if err != nil {
		return nil, err
	}

	disk, err := disk.Usage(a.config.Storage.DataPath)
	if err != nil {
		return nil, err
	}

	return &schedulerv1.AnnounceHostRequest{
		Id:                a.hostID,
		Type:              hostType,
		Hostname:          a.config.Host.Hostname,
		Ip:                a.config.Host.AdvertiseIP.String(),
		Port:              a.daemonPort,
		DownloadPort:      a.daemonDownloadPort,
		ObjectStoragePort: objectStoragePort,
		Os:                h.OS,
		Platform:          h.Platform,
		PlatformFamily:    h.PlatformFamily,
		PlatformVersion:   h.PlatformVersion,
		KernelVersion:     h.KernelVersion,
		Cpu: &schedulerv1.CPU{
			LogicalCount:   uint32(cpuLogicalCount),
			PhysicalCount:  uint32(cpuPhysicalCount),
			Percent:        cpuPercent[0],
			ProcessPercent: procCPUPercent,
			Times: &schedulerv1.CPUTimes{
				User:      cpuTimes[0].User,
				System:    cpuTimes[0].System,
				Idle:      cpuTimes[0].Idle,
				Nice:      cpuTimes[0].Nice,
				Iowait:    cpuTimes[0].Iowait,
				Irq:       cpuTimes[0].Irq,
				Softirq:   cpuTimes[0].Softirq,
				Steal:     cpuTimes[0].Steal,
				Guest:     cpuTimes[0].Guest,
				GuestNice: cpuTimes[0].GuestNice,
			},
		},
		Memory: &schedulerv1.Memory{
			Total:              virtualMemory.Total,
			Available:          virtualMemory.Available,
			Used:               virtualMemory.Used,
			UsedPercent:        virtualMemory.UsedPercent,
			ProcessUsedPercent: float64(procMemoryPercent),
			Free:               virtualMemory.Free,
		},
		Network: &schedulerv1.Network{
			TcpConnectionCount:       uint32(len(tcpConnections)),
			UploadTcpConnectionCount: uint32(len(uploadTCPConnections)),
			Location:                 a.config.Host.Location,
			Idc:                      a.config.Host.IDC,
		},
		Disk: &schedulerv1.Disk{
			Total:             disk.Total,
			Free:              disk.Free,
			Used:              disk.Used,
			UsedPercent:       disk.UsedPercent,
			InodesTotal:       disk.InodesTotal,
			InodesUsed:        disk.InodesUsed,
			InodesFree:        disk.InodesFree,
			InodesUsedPercent: disk.InodesUsedPercent,
		},
		Build: &schedulerv1.Build{
			GitVersion: version.GitVersion,
			GitCommit:  version.GitCommit,
			GoVersion:  version.GoVersion,
			Platform:   version.Platform,
		},
		SchedulerClusterId: a.dynconfig.GetSchedulerClusterID(),
	}, nil
}

// announceSeedPeer announces peer information to manager.
func (a *announcer) announceToManager() error {
	// Accounce seed peer information to manager.
	if a.config.Scheduler.Manager.SeedPeer.Enable {
		var objectStoragePort int32
		if a.config.ObjectStorage.Enable {
			objectStoragePort = int32(a.config.ObjectStorage.TCPListen.PortRange.Start)
		}

		if _, err := a.managerClient.UpdateSeedPeer(context.Background(), &managerv1.UpdateSeedPeerRequest{
			SourceType:        managerv1.SourceType_SEED_PEER_SOURCE,
			Hostname:          a.config.Host.Hostname,
			Type:              a.config.Scheduler.Manager.SeedPeer.Type,
			Idc:               a.config.Host.IDC,
			Location:          a.config.Host.Location,
			Ip:                a.config.Host.AdvertiseIP.String(),
			Port:              a.daemonPort,
			DownloadPort:      a.daemonDownloadPort,
			ObjectStoragePort: objectStoragePort,
			SeedPeerClusterId: uint64(a.config.Scheduler.Manager.SeedPeer.ClusterID),
		}); err != nil {
			return err
		}

		// Start keepalive to manager.
		go a.managerClient.KeepAlive(a.config.Scheduler.Manager.SeedPeer.KeepAlive.Interval, &managerv1.KeepAliveRequest{
			SourceType: managerv1.SourceType_SEED_PEER_SOURCE,
			Hostname:   a.config.Host.Hostname,
			Ip:         a.config.Host.AdvertiseIP.String(),
			ClusterId:  uint64(a.config.Scheduler.Manager.SeedPeer.ClusterID),
		}, a.done)
	}

	return nil
}
