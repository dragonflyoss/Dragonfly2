/*
 *     Copyright 2024 The Dragonfly Authors
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

//go:generate mockgen -destination host_manager_mock.go -source host_manager.go -package persistentcache

package persistentcache

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	pkgredis "d7y.io/dragonfly/v2/pkg/redis"
	pkgtypes "d7y.io/dragonfly/v2/pkg/types"
	"d7y.io/dragonfly/v2/scheduler/config"
)

// HostManager is the interface used for host manager.
type HostManager interface {
	// Load returns host for a key.
	Load(context.Context, string) (*Host, bool)

	// Store sets host.
	Store(context.Context, *Host)

	// Delete deletes host for a key.
	Delete(context.Context, string)

	// LoadAll returns all hosts.
	LoadAll(context.Context) ([]*Host, error)
}

// hostManager contains content for host manager.
type hostManager struct {
	// Config is scheduler config.
	config *config.Config

	// Redis universal client interface.
	rdb redis.UniversalClient
}

// TODO: Use newTaskManager for resource management.
// New host manager interface.
// nolint
func newHostManager(cfg *config.Config, rdb redis.UniversalClient) HostManager {
	return &hostManager{config: cfg, rdb: rdb}
}

// Load returns host for a key.
func (t *hostManager) Load(ctx context.Context, hostID string) (*Host, bool) {
	rawHost, err := t.rdb.HGetAll(ctx, pkgredis.MakePersistentCacheHostKeyInScheduler(t.config.Manager.SchedulerClusterID, hostID)).Result()
	if err != nil {
		fmt.Println("getting host failed from Redis:", err)
		return nil, false
	}

	// Set integer fields from raw host.
	port, err := strconv.ParseInt(rawHost["port"], 10, 32)
	if err != nil {
		fmt.Println("parsing port failed:", err)
		return nil, false
	}

	downloadPort, err := strconv.ParseInt(rawHost["download_port"], 10, 32)
	if err != nil {
		fmt.Println("parsing download port failed:", err)
		return nil, false
	}

	concurrentUploadLimit, err := strconv.ParseInt(rawHost["concurrent_upload_limit"], 10, 32)
	if err != nil {
		fmt.Println("parsing concurrent upload limit failed:", err)
		return nil, false
	}

	concurrentUploadCount, err := strconv.ParseInt(rawHost["concurrent_upload_count"], 10, 32)
	if err != nil {
		fmt.Println("parsing concurrent upload count failed:", err)
		return nil, false
	}

	uploadCount, err := strconv.ParseInt(rawHost["upload_count"], 10, 64)
	if err != nil {
		fmt.Println("parsing upload count failed:", err)
		return nil, false
	}

	uploadFailedCount, err := strconv.ParseInt(rawHost["upload_failed_count"], 10, 64)
	if err != nil {
		fmt.Println("parsing upload failed count failed:", err)
		return nil, false
	}

	// Set boolean fields from raw host.
	diableShared, err := strconv.ParseBool(rawHost["disable_shared"])
	if err != nil {
		fmt.Println("parsing disable shared failed:", err)
		return nil, false
	}

	// Set cpu fields from raw host.
	cpuLogicalCount, err := strconv.ParseUint(rawHost["cpu_logical_count"], 10, 32)
	if err != nil {
		fmt.Println("parsing cpu logical count failed:", err)
		return nil, false
	}

	cpuPhysicalCount, err := strconv.ParseUint(rawHost["cpu_physical_count"], 10, 32)
	if err != nil {
		fmt.Println("parsing cpu physical count failed:", err)
		return nil, false
	}

	cpuPercent, err := strconv.ParseFloat(rawHost["cpu_percent"], 64)
	if err != nil {
		fmt.Println("parsing cpu percent failed:", err)
		return nil, false
	}

	cpuProcessPercent, err := strconv.ParseFloat(rawHost["cpu_processe_percent"], 64)
	if err != nil {
		fmt.Println("parsing cpu process percent failed:", err)
		return nil, false
	}

	cpuTimesUser, err := strconv.ParseFloat(rawHost["cpu_times_user"], 64)
	if err != nil {
		fmt.Println("parsing cpu times user failed:", err)
		return nil, false
	}

	cpuTimesSystem, err := strconv.ParseFloat(rawHost["cpu_times_system"], 64)
	if err != nil {
		fmt.Println("parsing cpu times system failed:", err)
		return nil, false
	}

	cpuTimesIdle, err := strconv.ParseFloat(rawHost["cpu_times_idle"], 64)
	if err != nil {
		fmt.Println("parsing cpu times idle failed:", err)
		return nil, false
	}

	cpuTimesNice, err := strconv.ParseFloat(rawHost["cpu_times_nice"], 64)
	if err != nil {
		fmt.Println("parsing cpu times nice failed:", err)
		return nil, false
	}

	cpuTimesIowait, err := strconv.ParseFloat(rawHost["cpu_times_iowait"], 64)
	if err != nil {
		fmt.Println("parsing cpu times iowait failed:", err)
		return nil, false
	}

	cpuTimesIrq, err := strconv.ParseFloat(rawHost["cpu_times_irq"], 64)
	if err != nil {
		fmt.Println("parsing cpu times irq failed:", err)
		return nil, false
	}

	cpuTimesSoftirq, err := strconv.ParseFloat(rawHost["cpu_times_softirq"], 64)
	if err != nil {
		fmt.Println("parsing cpu times softirq failed:", err)
		return nil, false
	}

	cpuTimesSteal, err := strconv.ParseFloat(rawHost["cpu_times_steal"], 64)
	if err != nil {
		fmt.Println("parsing cpu times steal failed:", err)
		return nil, false
	}

	cpuTimesGuest, err := strconv.ParseFloat(rawHost["cpu_times_guest"], 64)
	if err != nil {
		fmt.Println("parsing cpu times guest failed:", err)
		return nil, false
	}

	cpuTimesGuestNice, err := strconv.ParseFloat(rawHost["cpu_times_guest_nice"], 64)
	if err != nil {
		fmt.Println("parsing cpu times guest nice failed:", err)
		return nil, false
	}

	cpu := CPU{
		LogicalCount:   uint32(cpuLogicalCount),
		PhysicalCount:  uint32(cpuPhysicalCount),
		Percent:        cpuPercent,
		ProcessPercent: cpuProcessPercent,
		Times: CPUTimes{
			User:      cpuTimesUser,
			System:    cpuTimesSystem,
			Idle:      cpuTimesIdle,
			Nice:      cpuTimesNice,
			Iowait:    cpuTimesIowait,
			Irq:       cpuTimesIrq,
			Softirq:   cpuTimesSoftirq,
			Steal:     cpuTimesSteal,
			Guest:     cpuTimesGuest,
			GuestNice: cpuTimesGuestNice,
		},
	}

	// Set memory fields from raw host.
	memoryTotal, err := strconv.ParseUint(rawHost["memory_total"], 10, 64)
	if err != nil {
		fmt.Println("parsing memory total failed:", err)
		return nil, false
	}

	memoryAvailable, err := strconv.ParseUint(rawHost["memory_available"], 10, 64)
	if err != nil {
		fmt.Println("parsing memory available failed:", err)
		return nil, false
	}

	memoryUsed, err := strconv.ParseUint(rawHost["memory_used"], 10, 64)
	if err != nil {
		fmt.Println("parsing memory used failed:", err)
		return nil, false
	}

	memoryUsedPercent, err := strconv.ParseFloat(rawHost["memory_used_percent"], 64)
	if err != nil {
		fmt.Println("parsing memory used percent failed:", err)
		return nil, false
	}

	memoryProcessUsedPercent, err := strconv.ParseFloat(rawHost["memory_processe_used_percent"], 64)
	if err != nil {
		fmt.Println("parsing memory process used percent failed:", err)
		return nil, false
	}

	memoryFree, err := strconv.ParseUint(rawHost["memory_free"], 10, 64)
	if err != nil {
		fmt.Println("parsing memory free failed:", err)
		return nil, false
	}

	memory := Memory{
		Total:              memoryTotal,
		Available:          memoryAvailable,
		Used:               memoryUsed,
		UsedPercent:        memoryUsedPercent,
		ProcessUsedPercent: memoryProcessUsedPercent,
		Free:               memoryFree,
	}

	// Set network fields from raw host.
	networkTCPConnectionCount, err := strconv.ParseUint(rawHost["network_tcp_connection_count"], 10, 32)
	if err != nil {
		fmt.Println("parsing network tcp connection count failed:", err)
		return nil, false
	}

	networkUploadTCPConnectionCount, err := strconv.ParseUint(rawHost["network_upload_tcp_connection_count"], 10, 32)
	if err != nil {
		fmt.Println("parsing network upload tcp connection count failed:", err)
		return nil, false
	}

	downloadRate, err := strconv.ParseUint(rawHost["network_download_rate"], 10, 64)
	if err != nil {
		fmt.Println("parsing download rate failed:", err)
		return nil, false
	}

	downloadRateLimit, err := strconv.ParseUint(rawHost["network_download_rate_limit"], 10, 64)
	if err != nil {
		fmt.Println("parsing download rate limit failed:", err)
		return nil, false
	}

	uploadRate, err := strconv.ParseUint(rawHost["network_upload_rate"], 10, 64)
	if err != nil {
		fmt.Println("parsing upload rate failed:", err)
		return nil, false
	}

	uploadRateLimit, err := strconv.ParseUint(rawHost["network_upload_rate_limit"], 10, 64)
	if err != nil {
		fmt.Println("parsing upload rate limit failed:", err)
		return nil, false
	}

	network := Network{
		TCPConnectionCount:       uint32(networkTCPConnectionCount),
		UploadTCPConnectionCount: uint32(networkUploadTCPConnectionCount),
		Location:                 rawHost["network_location"],
		IDC:                      rawHost["network_idc"],
		DownloadRate:             downloadRate,
		DownloadRateLimit:        downloadRateLimit,
		UploadRate:               uploadRate,
		UploadRateLimit:          uploadRateLimit,
	}

	// Set disk fields from raw host.
	diskTotal, err := strconv.ParseUint(rawHost["disk_total"], 10, 64)
	if err != nil {
		fmt.Println("parsing disk total failed:", err)
		return nil, false
	}

	diskFree, err := strconv.ParseUint(rawHost["disk_free"], 10, 64)
	if err != nil {
		fmt.Println("parsing disk free failed:", err)
		return nil, false
	}

	diskUsed, err := strconv.ParseUint(rawHost["disk_used"], 10, 64)
	if err != nil {
		fmt.Println("parsing disk used failed:", err)
		return nil, false
	}

	diskUsedPercent, err := strconv.ParseFloat(rawHost["disk_used_percent"], 64)
	if err != nil {
		fmt.Println("parsing disk used percent failed:", err)
		return nil, false
	}

	diskInodesTotal, err := strconv.ParseUint(rawHost["disk_inodes_total"], 10, 64)
	if err != nil {
		fmt.Println("parsing disk inodes total failed:", err)
		return nil, false
	}

	diskInodesUsed, err := strconv.ParseUint(rawHost["disk_inodes_used"], 10, 64)
	if err != nil {
		fmt.Println("parsing disk inodes used failed:", err)
		return nil, false
	}

	diskInodesFree, err := strconv.ParseUint(rawHost["disk_inodes_free"], 10, 64)
	if err != nil {
		fmt.Println("parsing disk inodes free failed:", err)
		return nil, false
	}

	diskInodesUsedPercent, err := strconv.ParseFloat(rawHost["disk_inodes_used_percent"], 64)
	if err != nil {
		fmt.Println("parsing disk inodes used percent failed:", err)
		return nil, false
	}

	disk := Disk{
		Total:             diskTotal,
		Free:              diskFree,
		Used:              diskUsed,
		UsedPercent:       diskUsedPercent,
		InodesTotal:       diskInodesTotal,
		InodesUsed:        diskInodesUsed,
		InodesFree:        diskInodesFree,
		InodesUsedPercent: diskInodesUsedPercent,
	}

	build := Build{
		GitVersion: rawHost["build_git_version"],
		GitCommit:  rawHost["build_git_commit"],
		GoVersion:  rawHost["build_go_version"],
		Platform:   rawHost["build_platform"],
	}

	// Set time fields from raw host.
	announceInterval, err := strconv.ParseInt(rawHost["announce_interval"], 10, 32)
	if err != nil {
		fmt.Println("parsing announce interval failed:", err)
		return nil, false
	}

	createdAt, err := time.Parse(time.RFC3339, rawHost["created_at"])
	if err != nil {
		fmt.Println("parsing created at failed:", err)
		return nil, false
	}

	updatedAt, err := time.Parse(time.RFC3339, rawHost["updated_at"])
	if err != nil {
		fmt.Println("parsing updated at failed:", err)
		return nil, false
	}

	return NewHost(
		rawHost["id"],
		rawHost["hostname"],
		rawHost["ip"],
		rawHost["os"],
		rawHost["platform"],
		rawHost["platform_family"],
		rawHost["platform_version"],
		rawHost["kernel_version"],
		int32(port),
		int32(downloadPort),
		int32(concurrentUploadLimit),
		int32(concurrentUploadCount),
		uploadCount,
		uploadFailedCount,
		diableShared,
		pkgtypes.ParseHostType(rawHost["type"]),
		cpu,
		memory,
		network,
		disk,
		build,
		time.Duration(announceInterval),
		createdAt,
		updatedAt,
		logger.WithHost(rawHost["id"], rawHost["hostname"], rawHost["ip"]),
	), true
}

// Store sets host.
func (t *hostManager) Store(ctx context.Context, host *Host) {
	t.rdb.HSet(ctx,
		pkgredis.MakePersistentCacheHostKeyInScheduler(t.config.Manager.SchedulerClusterID, host.ID),
		"id", host.ID,
		"type", host.Type.Name(),
		"hostname", host.Hostname,
		"ip", host.IP,
		"port", host.Port,
		"download_port", host.DownloadPort,
		"disable_shared", host.DisableShared,
		"os", host.OS,
		"platform", host.Platform,
		"platform_family", host.PlatformFamily,
		"platform_version", host.PlatformVersion,
		"kernel_version", host.KernelVersion,
		"cpu_logical_count", host.CPU.LogicalCount,
		"cpu_physical_count", host.CPU.PhysicalCount,
		"cpu_percent", host.CPU.Percent,
		"cpu_processe_percent", host.CPU.ProcessPercent,
		"cpu_times_user", host.CPU.Times.User,
		"cpu_times_system", host.CPU.Times.System,
		"cpu_times_idle", host.CPU.Times.Idle,
		"cpu_times_nice", host.CPU.Times.Nice,
		"cpu_times_iowait", host.CPU.Times.Iowait,
		"cpu_times_irq", host.CPU.Times.Irq,
		"cpu_times_softirq", host.CPU.Times.Softirq,
		"cpu_times_steal", host.CPU.Times.Steal,
		"cpu_times_guest", host.CPU.Times.Guest,
		"cpu_times_guest_nice", host.CPU.Times.GuestNice,
		"memory_total", host.Memory.Total,
		"memory_available", host.Memory.Available,
		"memory_used", host.Memory.Used,
		"memory_used_percent", host.Memory.UsedPercent,
		"memory_processe_used_percent", host.Memory.ProcessUsedPercent,
		"memory_free", host.Memory.Free,
		"network_tcp_connection_count", host.Network.TCPConnectionCount,
		"network_upload_tcp_connection_count", host.Network.UploadTCPConnectionCount,
		"network_location", host.Network.Location,
		"network_idc", host.Network.IDC,
		"network_download_rate", host.Network.DownloadRate,
		"network_download_rate_limit", host.Network.DownloadRateLimit,
		"network_upload_rate", host.Network.UploadRate,
		"network_upload_rate_limit", host.Network.UploadRateLimit,
		"disk_total", host.Disk.Total,
		"disk_free", host.Disk.Free,
		"disk_used", host.Disk.Used,
		"disk_used_percent", host.Disk.UsedPercent,
		"disk_inodes_total", host.Disk.InodesTotal,
		"disk_inodes_used", host.Disk.InodesUsed,
		"disk_inodes_free", host.Disk.InodesFree,
		"disk_inodes_used_percent", host.Disk.InodesUsedPercent,
		"build_git_version", host.Build.GitVersion,
		"build_git_commit", host.Build.GitCommit,
		"build_go_version", host.Build.GoVersion,
		"build_platform", host.Build.Platform,
		"announce_interval", host.AnnounceInterval,
		"concurrent_upload_limit", host.ConcurrentUploadLimit,
		"concurrent_upload_count", host.ConcurrentUploadCount,
		"upload_count", host.UploadCount,
		"upload_failed_count", host.UploadFailedCount,
		"created_at", host.CreatedAt.Format(time.RFC3339),
		"updated_at", host.UpdatedAt.Format(time.RFC3339))
}

// Delete deletes host for a key.
func (t *hostManager) Delete(ctx context.Context, hostID string) {
	t.rdb.Del(ctx, pkgredis.MakePersistentCacheHostKeyInScheduler(t.config.Manager.SchedulerClusterID, hostID))
}

// LoadAll returns all hosts.
func (t *hostManager) LoadAll(ctx context.Context) ([]*Host, error) {
	var (
		hosts  []*Host
		cursor uint64
	)

	for {
		var (
			hostKeys []string
			err      error
		)

		hostKeys, cursor, err = t.rdb.Scan(ctx, cursor, pkgredis.MakePersistentCacheHostsInScheduler(t.config.Manager.SchedulerClusterID), 10).Result()
		if err != nil {
			logger.Warn("scan hosts failed")
			return nil, err
		}

		for _, hostKey := range hostKeys {
			host, loaded := t.Load(ctx, hostKey)
			if !loaded {
				logger.WithHostID(hostKey).Warn("load host failed")
				continue
			}

			hosts = append(hosts, host)
		}

		if cursor == 0 {
			break
		}
	}

	return hosts, nil
}
