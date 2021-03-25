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
	"d7y.io/dragonfly/v2/cdnsystem/daemon/mgr/cdn/storage"
	"d7y.io/dragonfly/v2/cdnsystem/util"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"sync"
	"time"
)

func init() {
	// Ensure that Manager implements the GCMgr interface
	var manager *Manager = nil
	var _ mgr.GCMgr = manager
}

type Executor interface {
	GC(ctx context.Context) error
}

type ExecutorWrapper struct {
	GCInitialDelay time.Duration
	GCInterval     time.Duration
	Instance       Executor
}

var (
	gcExecutorWrappers = make(map[string]*ExecutorWrapper)
)

func Register(name string, gcWrapper *ExecutorWrapper) {
	gcExecutorWrappers[name] = gcWrapper
}

// Manager is an implementation of the interface of GCMgr.
type Manager struct {
	cfg     *config.Config
	taskMgr mgr.SeedTaskMgr
	cdnMgr  mgr.CDNMgr
	storage storage.StorageMgr
}

func (gcm *Manager) GCTask(ctx context.Context, taskID string, full bool) {
	logger.GcLogger.Infof("gc task: start to deal with task: %s", taskID)

	util.GetLock(taskID, false)
	defer util.ReleaseLock(taskID, false)

	var wg sync.WaitGroup
	wg.Add(2)

	go func(wg *sync.WaitGroup) {
		gcm.gcCDNByTaskID(ctx, taskID, full)
		wg.Done()
	}(&wg)

	// delete memory data
	go func(wg *sync.WaitGroup) {
		gcm.gcTaskByTaskID(ctx, taskID)
		wg.Done()
	}(&wg)

	wg.Wait()
	gcm.gcTask(ctx, taskID, full)
}

// NewManager returns a new Manager.
func NewManager(cfg *config.Config, taskMgr mgr.SeedTaskMgr, cdnMgr mgr.CDNMgr,
	storage storage.StorageMgr) (*Manager, error) {
	return &Manager{
		cfg:     cfg,
		taskMgr: taskMgr,
		cdnMgr:  cdnMgr,
		storage: storage,
	}, nil
}

// StartGC starts to do the gc jobs.
func (gcm *Manager) StartGC(ctx context.Context) {
	logger.Debugf("start the gc job")

	for name, executorWrapper := range gcExecutorWrappers {
		// start a goroutine to gc memory
		go func(name string, wrapper *ExecutorWrapper) {
			logger.Debugf("start the %s gc task", name)
			// delay to execute GC after gcm.initialDelay
			time.Sleep(wrapper.GCInitialDelay)

			// execute the GC by fixed delay
			ticker := time.NewTicker(wrapper.GCInterval)
			for range ticker.C {
				wrapper.Instance.GC(ctx)
			}
		}(name, executorWrapper)
	}
}
