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
	"d7y.io/dragonfly/v2/cdnsystem/cdnerrors"
	"d7y.io/dragonfly/v2/cdnsystem/config"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/mgr"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/mgr/gc"
	"d7y.io/dragonfly/v2/cdnsystem/source"
	"d7y.io/dragonfly/v2/cdnsystem/types"
	"d7y.io/dragonfly/v2/cdnsystem/util"
	"d7y.io/dragonfly/v2/pkg/dferrors"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/structure/syncmap"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
	"fmt"
	"github.com/pkg/errors"
	"sync"
	"time"
)

func init() {
	// Ensure that Manager implements the SeedTaskMgr interface
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
	resourceClient          source.ResourceClient
	cdnMgr                  mgr.CDNMgr
	progressMgr             mgr.SeedProgressMgr
}

// NewManager returns a new Manager Object.
func NewManager(cfg *config.Config, cdnMgr mgr.CDNMgr, progressMgr mgr.SeedProgressMgr,
	resourceClient source.ResourceClient) (*Manager, error) {
	taskMgr := &Manager{
		cfg:                     cfg,
		taskStore:               syncmap.NewSyncMap(),
		accessTimeMap:           syncmap.NewSyncMap(),
		taskURLUnReachableStore: syncmap.NewSyncMap(),
		resourceClient:          resourceClient,
		cdnMgr:                  cdnMgr,
		progressMgr:             progressMgr,
	}
	gc.Register("task", &gc.ExecutorWrapper{
		GCInitialDelay: cfg.GCInitialDelay,
		GCInterval:     cfg.GCMetaInterval,
		Instance:       taskMgr,
	})
	return taskMgr, nil
}

func (tm *Manager) Register(ctx context.Context, req *types.TaskRegisterRequest) (pieceChan <-chan *types.SeedPiece,
	err error) {
	task, err := tm.addOrUpdateTask(ctx, req)
	if err != nil {
		logger.WithTaskID(req.TaskId).Infof("failed to add or update task with req %+v: %v", req, err)
		return nil, err
	}
	// update accessTime for taskId
	if err := tm.accessTimeMap.Add(task.TaskId, time.Now()); err != nil {
		logger.WithTaskID(task.TaskId).Warnf("failed to update accessTime: %v", err)
	}
	logger.WithTaskID(task.TaskId).Debugf("success get task info: %+v", task)

	// trigger CDN
	if err := tm.triggerCdnSyncAction(ctx, task); err != nil {
		return nil, errors.Wrapf(err, "failed to trigger cdn")
	}
	// watch seed progress
	return tm.progressMgr.WatchSeedProgress(ctx, task.TaskId)
}

// triggerCdnSyncAction
func (tm *Manager) triggerCdnSyncAction(ctx context.Context, task *types.SeedTask) error {
	util.GetLock(task.TaskId, true)
	if !task.IsFrozen() {
		logger.WithTaskID(task.TaskId).Infof("seedTask is running or has been downloaded successfully, status:%s", task.CdnStatus)
		util.ReleaseLock(task.TaskId, true)
		return nil
	}
	util.ReleaseLock(task.TaskId, true)

	util.GetLock(task.TaskId, false)
	defer util.ReleaseLock(task.TaskId, false)
	// reconfirm
	if !task.IsFrozen() {
		logger.WithTaskID(task.TaskId).Infof("reconfirm seedTask is running or has been downloaded successfully, " +
			"status:%s", task.CdnStatus)
		return nil
	}
	if task.IsWait() {
		tm.progressMgr.InitSeedProgress(ctx, task.TaskId)
		logger.WithTaskID(task.TaskId).Infof("success to init seed progress")
	}

	updatedTask, err := tm.updateTask(task.TaskId, &types.SeedTask{
		CdnStatus: types.TaskInfoCdnStatusRUNNING,
	})
	if err != nil {
		return errors.Wrapf(err, "update task failed")
	}
	// triggerCDN goroutine
	go func() {
		updateTaskInfo, err := tm.cdnMgr.TriggerCDN(ctx, task)
		if err != nil {
			logger.WithTaskID(task.TaskId).Errorf("trigger cdn get error: %v", err)
		}
		go tm.progressMgr.PublishTask(ctx, task.TaskId, updateTaskInfo)
		updatedTask, err = tm.updateTask(task.TaskId, updateTaskInfo)
		if err != nil {
			logger.WithTaskID(task.TaskId).Errorf("update task fail:%v", err)
		} else {
			logger.WithTaskID(task.TaskId).Infof("success to update task cdn updatedTask:%+v", updatedTask)
		}
	}()
	logger.WithTaskID(task.TaskId).Infof("success start cdn trigger")
	return nil
}

func (tm *Manager) getTask(taskId string) (*types.SeedTask, error) {
	if stringutils.IsBlank(taskId) {
		return nil, errors.Wrap(cdnerrors.ErrEmptyValue, "taskId is empty")
	}

	v, err := tm.taskStore.Get(taskId)
	if err != nil {
		if errors.Cause(err) == dferrors.ErrDataNotFound {
			return nil, errors.Wrapf(cdnerrors.ErrDataNotFound,"task not found")
		}
		return nil, err
	}
	// update accessTime for taskId
	if err := tm.accessTimeMap.Add(taskId, time.Now()); err != nil {
		logger.WithTaskID(taskId).Warnf("failed to update accessTime: %v", err)
	}
	// type assertion
	if info, ok := v.(*types.SeedTask); ok {
		return info, nil
	}
	return nil, errors.Wrapf(cdnerrors.ErrConvertFailed, "taskId %s: %v", taskId, v)
}

func (tm Manager) Get(ctx context.Context, taskId string) (*types.SeedTask, error) {
	// todo locker
	return tm.getTask(taskId)
}

func (tm Manager) GetAccessTime(ctx context.Context) (*syncmap.SyncMap, error) {
	return tm.accessTimeMap, nil
}

func (tm Manager) Delete(ctx context.Context, taskId string) error {
	tm.accessTimeMap.Delete(taskId)
	tm.taskURLUnReachableStore.Delete(taskId)
	tm.taskStore.Delete(taskId)
	return nil
}

func (tm *Manager) GetPieces(ctx context.Context, taskId string) (pieces []*types.SeedPiece, err error) {
	util.GetLock(taskId, true)
	defer util.ReleaseLock(taskId, true)
	return tm.progressMgr.GetPieces(ctx, taskId)
}

const (
	// gcTasksTimeout specifies the timeout for tasks gc.
	// If the actual execution time exceeds this threshold, a warning will be thrown.
	gcTasksTimeout = 2.0 * time.Second
)

func (tm *Manager) GC(ctx context.Context) error {
	logger.Debugf("start the meta gc job")
	var removedTaskCount int
	startTime := time.Now()
	// get all taskIDs and the corresponding accessTime
	taskAccessMap, err := tm.GetAccessTime(ctx)
	if err != nil {
		return fmt.Errorf("gc tasks: failed to get task accessTime map for GC: %v", err)
	}

	// range all tasks and determine whether they are expired
	taskIDs := taskAccessMap.ListKeyAsStringSlice()
	totalTaskNums := len(taskIDs)
	for _, taskID := range taskIDs {
		atime, err := taskAccessMap.GetAsTime(taskID)
		if err != nil {
			logger.GcLogger.Errorf("gc tasks: failed to get access time taskID(%s): %v", taskID, err)
			continue
		}
		if time.Since(atime) < tm.cfg.TaskExpireTime {
			continue
		}
		// gc task memory data
		tm.gcTask(ctx, taskID, false)
		removedTaskCount++
	}

	// slow GC detected, report it with a log warning
	if timeDuring := time.Since(startTime); timeDuring > gcTasksTimeout {
		logger.GcLogger.Warnf("gc tasks:%d cost:%.3f", removedTaskCount, timeDuring.Seconds())
	}
	logger.GcLogger.Infof("gc tasks: success to full gc task count(%d), remainder count(%d)", removedTaskCount, totalTaskNums-removedTaskCount)
	return nil
}

// gcTask
func (tm *Manager) gcTask(ctx context.Context, taskID string, full bool) {
	logger.GcLogger.Infof("gc task: start to deal with task: %s", taskID)

	util.GetLock(taskID, false)
	defer util.ReleaseLock(taskID, false)

	var wg sync.WaitGroup
	wg.Add(2)

	go func(wg *sync.WaitGroup) {
		tm.gcCDNByTaskID(ctx, taskID, full)
		wg.Done()
	}(&wg)

	// delete memory data
	go func(wg *sync.WaitGroup) {
		tm.gcTaskByTaskID(ctx, taskID)
		wg.Done()
	}(&wg)

	wg.Wait()
}

// gcCDNByTaskID
func (tm *Manager) gcCDNByTaskID(ctx context.Context, taskID string, force bool) {
	if err := tm.cdnMgr.Delete(ctx, taskID, force); err != nil {
		logger.GcLogger.Errorf("gc task: failed to gc cdn meta taskID(%s): %v", taskID, err)
	}
}

// gcTaskByTaskID
func (tm *Manager) gcTaskByTaskID(ctx context.Context, taskID string) {
	if err := tm.Delete(ctx, taskID); err != nil {
		logger.GcLogger.Errorf("gc task: failed to gc task info taskID(%s): %v", taskID, err)
	}
}
