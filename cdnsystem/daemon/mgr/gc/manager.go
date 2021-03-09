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
	"time"

	"d7y.io/dragonfly/v2/cdnsystem/config"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/mgr"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"github.com/prometheus/client_golang/prometheus"
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

// Manager is an implementation of the interface of GCMgr.
type Manager struct {
	cfg     *config.Config
	taskMgr mgr.SeedTaskMgr
	cdnMgr  mgr.CDNMgr
}

// NewManager returns a new Manager.
func NewManager(cfg *config.Config, taskMgr mgr.SeedTaskMgr, cdnMgr mgr.CDNMgr, register prometheus.Registerer) (*Manager, error) {
	return &Manager{
		cfg:     cfg,
		taskMgr: taskMgr,
		cdnMgr:  cdnMgr,
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
}

// GCTask is used to do the gc job with specified taskID.
// The CDN file will be deleted when the full is true.
func (gcm *Manager) GCTask(ctx context.Context, taskID string, full bool) {
	gcm.gcTask(ctx, taskID, full)
}
