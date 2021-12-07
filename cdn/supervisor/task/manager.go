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
	"encoding/json"
	"time"

	"github.com/pkg/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"d7y.io/dragonfly/v2/cdn/config"
	cdnerrors "d7y.io/dragonfly/v2/cdn/errors"
	"d7y.io/dragonfly/v2/cdn/supervisor"
	"d7y.io/dragonfly/v2/cdn/supervisor/gc"
	"d7y.io/dragonfly/v2/cdn/types"
	"d7y.io/dragonfly/v2/internal/dferrors"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/util"
	"d7y.io/dragonfly/v2/pkg/source"
	"d7y.io/dragonfly/v2/pkg/synclock"
	"d7y.io/dragonfly/v2/pkg/syncmap"
	"d7y.io/dragonfly/v2/pkg/unit"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
)

// Ensure that Manager implements the SeedTaskMgr and gcExecutor interfaces
var _ supervisor.SeedTaskMgr = (*Manager)(nil)
var _ gc.Executor = (*Manager)(nil)

var (
	errURLUnreachable = errors.New("url is unreachable")
	errTaskIDConflict = errors.New("taskID is conflict")
)

var tracer trace.Tracer

func init() {
	tracer = otel.Tracer("cdn-task-manager")
}

// Manager is an implementation of the interface of TaskMgr.
type Manager struct {
	cfg                     *config.Config
	taskStore               *syncmap.SyncMap
	accessTimeMap           *syncmap.SyncMap
	taskURLUnReachableStore *syncmap.SyncMap
	cdnMgr                  supervisor.CDNMgr
	progressMgr             supervisor.SeedProgressMgr
}

// NewManager returns a new Manager Object.
func NewManager(cfg *config.Config, cdnMgr supervisor.CDNMgr, progressMgr supervisor.SeedProgressMgr) (*Manager, error) {
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

func (tm *Manager) Register(ctx context.Context, registerTask *types.SeedTask) (pieceChan <-chan *types.SeedPiece, err error) {
	var span trace.Span
	ctx, span = tracer.Start(ctx, config.SpanTaskRegister)
	defer span.End()
	task, err := tm.AddOrUpdate(registerTask)
	if err != nil {
		span.RecordError(err)
		logger.WithTaskID(registerTask.TaskID).Infof("failed to add or update task with req: %#v: %v", registerTask, err)
		return nil, err
	}
	taskBytes, _ := json.Marshal(task)
	span.SetAttributes(config.AttributeTaskInfo.String(string(taskBytes)))
	task.Log().Debugf("success get task info: %#v", task)

	// update accessTime for taskId
	if err := tm.accessTimeMap.Add(task.TaskID, time.Now()); err != nil {
		task.Log().Warnf("failed to update accessTime: %v", err)
	}

	// trigger CDN
	if err := tm.triggerCdnSyncAction(ctx, task); err != nil {
		return nil, errors.Wrapf(err, "trigger cdn")
	}
	task.Log().Infof("successfully trigger cdn sync action")
	// watch seed progress
	return tm.progressMgr.WatchSeedProgress(ctx, task.TaskID)
}

// triggerCdnSyncAction
func (tm *Manager) triggerCdnSyncAction(ctx context.Context, task *types.SeedTask) error {
	var span trace.Span
	ctx, span = tracer.Start(ctx, config.SpanTriggerCDNSyncAction)
	defer span.End()
	synclock.Lock(task.TaskID, true)
	if !task.IsFrozen() {
		span.SetAttributes(config.AttributeTaskStatus.String(task.CdnStatus))
		task.Log().Infof("seedTask is running or has been downloaded successfully, status: %s", task.CdnStatus)
		synclock.UnLock(task.TaskID, true)
		return nil
	}
	synclock.UnLock(task.TaskID, true)

	synclock.Lock(task.TaskID, false)
	defer synclock.UnLock(task.TaskID, false)
	// reconfirm
	span.SetAttributes(config.AttributeTaskStatus.String(task.CdnStatus))
	if !task.IsFrozen() {
		task.Log().Infof("reconfirm find seedTask is running or has been downloaded successfully, status: %s", task.CdnStatus)
		return nil
	}
	if task.IsWait() {
		tm.progressMgr.InitSeedProgress(ctx, task.TaskID)
		task.Log().Infof("successfully init seed progress for task")
	}
	updatedTask, err := tm.updateTask(task.TaskID, &types.SeedTask{
		CdnStatus: types.TaskInfoCdnStatusRunning,
	})
	if err != nil {
		return errors.Wrapf(err, "update task")
	}
	// triggerCDN goroutine
	go func() {
		updateTaskInfo, err := tm.cdnMgr.TriggerCDN(ctx, task)
		if err != nil {
			task.Log().Errorf("trigger cdn get error: %v", err)
		}
		updatedTask, err = tm.updateTask(task.TaskID, updateTaskInfo)
		go func() {
			if err := tm.progressMgr.PublishTask(ctx, task.TaskID, updatedTask); err != nil {
				task.Log().Errorf("failed to publish task: %v", err)
			}

		}()
		if err != nil {
			task.Log().Errorf("failed to update task: %v", err)
		}
		task.Log().Infof("successfully update task cdn updatedTask: %#v", updatedTask)
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
	// type assertion
	if info, ok := v.(*types.SeedTask); ok {
		return info, nil
	}
	return nil, errors.Wrapf(cdnerrors.ErrConvertFailed, "origin object: %#v", v)
}

func (tm *Manager) AddOrUpdate(registerTask *types.SeedTask) (seedTask *types.SeedTask, err error) {
	defer func() {
		if err != nil {
			tm.accessTimeMap.Store(registerTask.TaskID, time.Now())
		}
	}()
	synclock.Lock(registerTask.TaskID, true)
	if unreachableTime, ok := tm.getTaskUnreachableTime(registerTask.TaskID); ok {
		if time.Since(unreachableTime) < tm.cfg.FailAccessInterval {
			synclock.UnLock(registerTask.TaskID, true)
			// TODO 校验Header
			return nil, errURLUnreachable
		}
		logger.Debugf("delete taskID: %s from unreachable url list", registerTask.TaskID)
		tm.taskURLUnReachableStore.Delete(registerTask.TaskID)
	}
	actual, loaded := tm.taskStore.LoadOrStore(registerTask.TaskID, registerTask)
	seedTask = actual.(*types.SeedTask)
	if loaded && !IsSame(seedTask, registerTask) {
		synclock.UnLock(registerTask.TaskID, true)
		return nil, errors.Wrapf(errTaskIDConflict, "register task %#v is conflict with exist task %#v", registerTask, seedTask)
	}
	if seedTask.SourceFileLength != source.UnKnownSourceFileLen {
		synclock.UnLock(registerTask.TaskID, true)
		return seedTask, nil
	}
	synclock.UnLock(registerTask.TaskID, true)
	synclock.Lock(registerTask.TaskID, false)
	defer synclock.UnLock(registerTask.TaskID, false)
	if seedTask.SourceFileLength != source.UnKnownSourceFileLen {
		return seedTask, nil
	}
	// get sourceContentLength with req.Header
	contentLengthRequest, err := source.NewRequestWithHeader(registerTask.URL, registerTask.Header)
	if err != nil {
		return nil, err
	}
	// add range info
	if !stringutils.IsBlank(registerTask.Range) {
		contentLengthRequest.Header.Add(source.Range, registerTask.Range)
	}
	sourceFileLength, err := source.GetContentLength(contentLengthRequest)
	if err != nil {
		registerTask.Log().Errorf("get url (%s) content length failed: %v", registerTask.URL, err)
		if source.IsResourceNotReachableError(err) {
			tm.taskURLUnReachableStore.Store(registerTask, time.Now())
		}
		return seedTask, err
	}
	seedTask.SourceFileLength = sourceFileLength
	seedTask.Log().Debugf("success get file content length: %d", sourceFileLength)

	// if success to get the information successfully with the req.Header then update the task.UrlMeta to registerTask.UrlMeta.
	if registerTask.Header != nil {
		seedTask.Header = registerTask.Header
	}

	// calculate piece size and update the PieceSize and PieceTotal
	if registerTask.PieceSize <= 0 {
		pieceSize := util.ComputePieceSize(registerTask.SourceFileLength)
		seedTask.PieceSize = int32(pieceSize)
		seedTask.Log().Debugf("piece size calculate result: %s", unit.ToBytes(int64(pieceSize)))
	}
	return seedTask, nil
}

func (tm Manager) Get(taskID string) (*types.SeedTask, error) {
	task, err := tm.getTask(taskID)
	// update accessTime for taskID
	if err := tm.accessTimeMap.Add(taskID, time.Now()); err != nil {
		logger.WithTaskID(taskID).Warnf("failed to update accessTime: %v", err)
	}
	return task, err
}

func (tm Manager) Exist(taskID string) (*types.SeedTask, bool) {
	task, err := tm.getTask(taskID)
	return task, err == nil
}

func (tm Manager) Delete(taskID string) error {
	tm.accessTimeMap.Delete(taskID)
	tm.taskURLUnReachableStore.Delete(taskID)
	tm.taskStore.Delete(taskID)
	if err := tm.progressMgr.Clear(taskID); err != nil {
		return err
	}
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
	taskAccessMap := tm.accessTimeMap

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
		if err := tm.Delete(taskID); err != nil {
			logger.GcLogger.With("type", "meta").Infof("gc task: failed to delete task: %s", taskID)
			continue
		}
		removedTaskCount++
	}

	// slow GC detected, report it with a log warning
	if timeDuring := time.Since(startTime); timeDuring > gcTasksTimeout {
		logger.GcLogger.With("type", "meta").Warnf("gc tasks: %d cost: %.3f", removedTaskCount, timeDuring.Seconds())
	}
	logger.GcLogger.With("type", "meta").Infof("gc tasks: successfully full gc task count(%d), remainder count(%d)", removedTaskCount, totalTaskNums-removedTaskCount)
	return nil
}
