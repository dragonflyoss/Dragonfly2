/*
 *     Copyright 2020 The Dragonfly Authors
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

package gc

import (
	"context"
	"d7y.io/dragonfly/v2/cdnsystem/config"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/mgr"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/util/fileutils"
	"d7y.io/dragonfly/v2/pkg/util/metricsutils"
	"github.com/prometheus/client_golang/prometheus"
	"time"
)

func init() {
	// Ensure that Manager implements the GCMgr interface
	var manager *Manager = nil
	var _ mgr.GCMgr = manager
}

type metrics struct {
	gcTasksCount    *prometheus.CounterVec
	gcDisksCount    *prometheus.CounterVec
	lastGCDisksTime *prometheus.GaugeVec
}

func newMetrics(register prometheus.Registerer) *metrics {
	return &metrics{
		gcTasksCount: metricsutils.NewCounter(config.SubsystemCdnSystem, "gc_tasks_total",
			"Total number of tasks that have been garbage collected", []string{}, register),

		gcDisksCount: metricsutils.NewCounter(config.SubsystemCdnSystem, "gc_disks_total",
			"Total number of garbage collecting the task data in disks", []string{}, register),

		lastGCDisksTime: metricsutils.NewGauge(config.SubsystemCdnSystem, "last_gc_disks_timestamp_seconds",
			"Timestamp of the last disk gc", []string{}, register),
	}
}

// Manager is an implementation of the interface of GCMgr.
type Manager struct {
	cfg     *config.Config
	taskMgr mgr.SeedTaskMgr
	cdnMgr  mgr.CDNMgr
	metrics *metrics
}

// NewManager returns a new Manager.
func NewManager(cfg *config.Config, taskMgr mgr.SeedTaskMgr, cdnMgr mgr.CDNMgr, register prometheus.Registerer) (*Manager, error) {
	return &Manager{
		cfg:     cfg,
		taskMgr: taskMgr,
		cdnMgr:  cdnMgr,
		metrics: newMetrics(register),
	}, nil
}

// StartGC starts to do the gc jobs.
func (gcm *Manager) StartGC(ctx context.Context) {
	logger.Debugf("start the gc job")

	// start a goroutine to gc memory
	go func() {
		// delay to execute GC after gcm.initialDelay
		time.Sleep(gcm.cfg.GCInitialDelay)

		// execute the GC by fixed delay
		ticker := time.NewTicker(gcm.cfg.GCMetaInterval)
		for range ticker.C {
			gcm.gcTasks(ctx)
		}
	}()

	// start a goroutine to gc the disks
	go func() {
		// delay to execute GC after gcm.initialDelay
		time.Sleep(gcm.cfg.GCInitialDelay)

		// execute the GC by fixed delay
		ticker := time.NewTicker(gcm.cfg.GCDiskInterval)
		for range ticker.C {
			gcm.gcDisk(ctx)
		}
	}()

	go func() {
		if !fileutils.PathExist(config.ShmHome) {
			return
		}
		fsize, err := fileutils.GetTotalSpace(config.ShmHome)
		if err != nil {
			logger.CoreLogger.Error()
			return
		}
		diff := fileutils.Fsize(0)
		if fsize < 72 * 1024 * 1024 * 1024 {
			diff = fileutils.Fsize(72 * 1024 * 1024 * 1024) - fsize
		}
		if diff >= fsize {
			return
		}

		time.Sleep(gcm.cfg.GCInitialDelay)

		// execute the GC by fixed delay
		ticker := time.NewTicker(gcm.cfg.GCShmInterval)
		for range ticker.C {
			gcm.gcShm(ctx)
		}
	}()
}

// GCTask is used to do the gc job with specified taskID.
// The CDN file will be deleted when the full is true.
func (gcm *Manager) GCTask(ctx context.Context, taskID string, full bool) {
	gcm.gcTask(ctx, taskID, full)
}
