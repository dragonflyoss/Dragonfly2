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

package task

import (
	"context"
	"fmt"
	"time"

	"d7y.io/dragonfly/v2/cdnsystem/cdnerrors"
	"d7y.io/dragonfly/v2/cdnsystem/config"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/mgr"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/mgr/gc"
	"d7y.io/dragonfly/v2/cdnsystem/types"
	"d7y.io/dragonfly/v2/pkg/dferrors"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/structure/syncmap"
	"d7y.io/dragonfly/v2/pkg/synclock"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
	"github.com/pkg/errors"
)

func init() {
	// Ensure that Manager implements the SeedTaskMgr and gcExecutor interfaces
	var manager *Manager = nil
	var _ mgr.SeedTaskMgr = manager
	var _ gc.Executor = manager
}

// Manager is an implementation of the interface of TaskMgr.
type Manager struct {
	cfg                     *config.Config
	taskStore               *syncmap.SyncMap
	accessTimeMap           *syncmap.SyncMap
	taskURLUnReachableStore *syncmap.SyncMap
	cdnMgr                  mgr.CDNMgr
	progressMgr             mgr.SeedProgressMgr
}

// NewManager returns a new Manager Object.
func NewManager(cfg *config.Config, cdnMgr mgr.CDNMgr, progressMgr mgr.SeedProgressMgr) (*Manager, error) {
	taskMgr := &Manager{
		cfg:                     cfg,
		taskStore:               syncmap.NewSyncMap(),
		accessTimeMap:           syncmap.NewSyncMap(),
		taskURLUnReachableStore: syncmap.NewSyncMap(),
		cdnMgr:                  cdnMgr,
		progressMgr:             progressMgr,
	}
	progressMgr.SetTaskMgr(taskMgr)
	gc.Register("task", cfg.GCInitialDelay, cfg.GCMetaInterval, taskMgr)
	return taskMgr, nil
}

func (tm *Manager) Register(ctx context.Context, req *types.TaskRegisterRequest) (pieceChan <-chan *types.SeedPiece, err error) {
	task, err := tm.addOrUpdateTask(ctx, req)
	if err != nil {
		logger.WithTaskID(req.TaskID).Infof("failed to add or update task with req: %+v: %v", req, err)
		return nil, err
	}
	logger.WithTaskID(task.TaskID).Debugf("success get task info: %+v", task)

	// update accessTime for taskId
	if err := tm.accessTimeMap.Add(task.TaskID, time.Now()); err != nil {
		logger.WithTaskID(task.TaskID).Warnf("failed to update accessTime: %v", err)
	}

	// trigger CDN
	if err := tm.triggerCdnSyncAction(ctx, task); err != nil {
		return nil, errors.Wrapf(err, "failed to trigger cdn")
	}
	logger.WithTaskID(task.TaskID).Infof("successfully trigger cdn sync action")
	// watch seed progress
	return tm.progressMgr.WatchSeedProgress(ctx, task.TaskID)
}

// triggerCdnSyncAction
func (tm *Manager) triggerCdnSyncAction(ctx context.Context, task *types.SeedTask) error {
	synclock.Lock(task.TaskID, true)
	if !task.IsFrozen() {
		logger.WithTaskID(task.TaskID).Infof("seedTask is running or has been downloaded successfully, status:%s", task.CdnStatus)
		synclock.UnLock(task.TaskID, true)
		return nil
	}
	synclock.UnLock(task.TaskID, true)

	synclock.Lock(task.TaskID, false)
	defer synclock.UnLock(task.TaskID, false)
	// reconfirm
	if !task.IsFrozen() {
		logger.WithTaskID(task.TaskID).Infof("reconfirm find seedTask is running or has been downloaded successfully, status:%s", task.CdnStatus)
		return nil
	}
	if task.IsWait() {
		tm.progressMgr.InitSeedProgress(ctx, task.TaskID)
		logger.WithTaskID(task.TaskID).Infof("successfully init seed progress for task")
	}

	updatedTask, err := tm.updateTask(task.TaskID, &types.SeedTask{
		CdnStatus: types.TaskInfoCdnStatusRunning,
	})
	if err != nil {
		return errors.Wrapf(err, "failed to update task")
	}
	// triggerCDN goroutine
	go func() {
		updateTaskInfo, err := tm.cdnMgr.TriggerCDN(ctx, task)
		if err != nil {
			logger.WithTaskID(task.TaskID).Errorf("trigger cdn get error: %v", err)
		}
		go func() {
			if err := tm.progressMgr.PublishTask(ctx, task.TaskID, updateTaskInfo); err != nil {
				logger.WithTaskID(task.TaskID).Errorf("failed to publish task: %v", err)
			}

		}()
		updatedTask, err = tm.updateTask(task.TaskID, updateTaskInfo)
		if err != nil {
			logger.WithTaskID(task.TaskID).Errorf("failed to update task:%v", err)
		}
		logger.WithTaskID(task.TaskID).Infof("successfully update task cdn updatedTask:%+v", updatedTask)
	}()
	return nil
}

func (tm *Manager) getTask(taskID string) (*types.SeedTask, error) {
	if stringutils.IsBlank(taskID) {
		return nil, errors.Wrap(cdnerrors.ErrInvalidValue, "taskID is empty")
	}

	v, err := tm.taskStore.Get(taskID)
	if err != nil {
		if errors.Cause(err) == dferrors.ErrDataNotFound {
			return nil, errors.Wrapf(cdnerrors.ErrDataNotFound, "task not found")
		}
		return nil, err
	}
	// update accessTime for taskID
	if err := tm.accessTimeMap.Add(taskID, time.Now()); err != nil {
		logger.WithTaskID(taskID).Warnf("failed to update accessTime: %v", err)
	}
	// type assertion
	if info, ok := v.(*types.SeedTask); ok {
		return info, nil
	}
	return nil, errors.Wrapf(cdnerrors.ErrConvertFailed, "origin object: %+v", v)
}

func (tm Manager) Get(taskID string) (*types.SeedTask, error) {
	return tm.getTask(taskID)
}

func (tm Manager) GetAccessTime() (*syncmap.SyncMap, error) {
	return tm.accessTimeMap, nil
}

func (tm Manager) Delete(taskID string) error {
	tm.accessTimeMap.Delete(taskID)
	tm.taskURLUnReachableStore.Delete(taskID)
	tm.taskStore.Delete(taskID)
	tm.progressMgr.Clear(taskID)
	return nil
}

func (tm *Manager) GetPieces(ctx context.Context, taskID string) (pieces []*types.SeedPiece, err error) {
	synclock.Lock(taskID, true)
	defer synclock.UnLock(taskID, true)
	return tm.progressMgr.GetPieces(ctx, taskID)
}

const (
	// gcTasksTimeout specifies the timeout for tasks gc.
	// If the actual execution time exceeds this threshold, a warning will be thrown.
	gcTasksTimeout = 2.0 * time.Second
)

func (tm *Manager) GC() error {
	logger.Debugf("start the task meta gc job")
	var removedTaskCount int
	startTime := time.Now()
	// get all taskIDs and the corresponding accessTime
	taskAccessMap, err := tm.GetAccessTime()
	if err != nil {
		return fmt.Errorf("gc tasks: failed to get task accessTime map for GC: %v", err)
	}

	// range all tasks and determine whether they are expired
	taskIDs := taskAccessMap.ListKeyAsStringSlice()
	totalTaskNums := len(taskIDs)
	for _, taskID := range taskIDs {
		atime, err := taskAccessMap.GetAsTime(taskID)
		if err != nil {
			logger.GcLogger.With("type", "meta").Errorf("gc tasks: failed to get access time taskID(%s): %v", taskID, err)
			continue
		}
		if time.Since(atime) < tm.cfg.TaskExpireTime {
			continue
		}
		// gc task memory data
		logger.GcLogger.With("type", "meta").Infof("gc task: start to deal with task: %s", taskID)
		tm.Delete(taskID)
		removedTaskCount++
	}

	// slow GC detected, report it with a log warning
	if timeDuring := time.Since(startTime); timeDuring > gcTasksTimeout {
		logger.GcLogger.With("type", "meta").Warnf("gc tasks:%d cost:%.3f", removedTaskCount, timeDuring.Seconds())
	}
	logger.GcLogger.With("type", "meta").Infof("gc tasks: successfully full gc task count(%d), remainder count(%d)", removedTaskCount, totalTaskNums-removedTaskCount)
	return nil
}
