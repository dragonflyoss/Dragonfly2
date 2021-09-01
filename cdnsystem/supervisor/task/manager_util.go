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
	"reflect"
	"time"

	"d7y.io/dragonfly/v2/cdnsystem/cdnutil"
	"d7y.io/dragonfly/v2/cdnsystem/config"
	cdnerrors "d7y.io/dragonfly/v2/cdnsystem/errors"
	"d7y.io/dragonfly/v2/cdnsystem/types"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/source"
	"d7y.io/dragonfly/v2/pkg/synclock"
	"d7y.io/dragonfly/v2/pkg/util/net/urlutils"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/trace"
)

const (
	IllegalSourceFileLen = -100
)

// addOrUpdateTask add a new task or update exist task
func (tm *Manager) addOrUpdateTask(ctx context.Context, request *types.TaskRegisterRequest) (*types.SeedTask, error) {
	var span trace.Span
	ctx, span = tracer.Start(ctx, config.SpanAndOrUpdateTask)
	defer span.End()
	taskURL := request.URL
	if request.Filter != nil {
		taskURL = urlutils.FilterURLParam(request.URL, request.Filter)
	}
	span.SetAttributes(config.AttributeTaskURL.String(taskURL))
	taskID := request.TaskID
	synclock.Lock(taskID, false)
	defer synclock.UnLock(taskID, false)
	if key, err := tm.taskURLUnReachableStore.Get(taskID); err == nil {
		if unReachableStartTime, ok := key.(time.Time); ok && time.Since(unReachableStartTime) < tm.cfg.FailAccessInterval {
			existTask, err := tm.taskStore.Get(taskID)
			if err != nil || reflect.DeepEqual(request.Header, existTask.(*types.SeedTask).Header) {
				span.AddEvent(config.EventHitUnReachableURL)
				return nil, errors.Wrapf(cdnerrors.ErrURLNotReachable{URL: request.URL}, "task hit unReachable cache and interval less than %d, "+
					"url: %s", tm.cfg.FailAccessInterval, request.URL)
			}
		}
		span.AddEvent(config.EventDeleteUnReachableTask)
		tm.taskURLUnReachableStore.Delete(taskID)
		logger.Debugf("delete taskID: %s from url unReachable store", taskID)
	}

	var task *types.SeedTask
	newTask := &types.SeedTask{
		TaskID:           taskID,
		Header:           request.Header,
		RequestDigest:    request.Digest,
		URL:              request.URL,
		TaskURL:          taskURL,
		CdnStatus:        types.TaskInfoCdnStatusWaiting,
		SourceFileLength: IllegalSourceFileLen,
	}
	// using the existing task if it already exists corresponding to taskID
	if v, err := tm.taskStore.Get(taskID); err == nil {
		span.SetAttributes(config.AttributeIfReuseTask.Bool(true))
		existTask := v.(*types.SeedTask)
		if !isSameTask(existTask, newTask) {
			span.RecordError(fmt.Errorf("newTask: %+v, existTask: %+v", newTask, existTask))
			return nil, cdnerrors.ErrTaskIDDuplicate{TaskID: taskID, Cause: fmt.Errorf("newTask: %+v, existTask: %+v", newTask, existTask)}
		}
		task = existTask
		logger.Debugf("get exist task for taskID: %s", taskID)
	} else {
		span.SetAttributes(config.AttributeIfReuseTask.Bool(false))
		logger.Debugf("get new task for taskID: %s", taskID)
		task = newTask
	}

	if task.SourceFileLength != IllegalSourceFileLen {
		return task, nil
	}

	// get sourceContentLength with req.Header
	ctx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	span.AddEvent(config.EventRequestSourceFileLength)
	sourceFileLength, err := source.GetContentLength(ctx, task.URL, request.Header)
	if err != nil {
		logger.WithTaskID(task.TaskID).Errorf("failed to get url (%s) content length: %v", task.URL, err)
		if cdnerrors.IsURLNotReachable(err) {
			tm.taskURLUnReachableStore.Add(taskID, time.Now())
			return nil, err
		}
	}
	// if not support file length header request ,return -1
	task.SourceFileLength = sourceFileLength
	logger.WithTaskID(taskID).Debugf("get file content length: %d", sourceFileLength)
	if task.SourceFileLength > 0 {
		ok, err := tm.cdnMgr.TryFreeSpace(task.SourceFileLength)
		if err != nil {
			logger.Errorf("failed to try free space: %v", err)
		} else if !ok {
			return nil, cdnerrors.ErrResourcesLacked
		}
	}

	// if success to get the information successfully with the req.Header then update the task.Header to req.Header.
	if request.Header != nil {
		task.Header = request.Header
	}

	// calculate piece size and update the PieceSize and PieceTotal
	if task.PieceSize <= 0 {
		pieceSize := cdnutil.ComputePieceSize(task.SourceFileLength)
		task.PieceSize = pieceSize
	}
	tm.taskStore.Add(task.TaskID, task)
	logger.Debugf("success add task: %+v into taskStore", task)

	return task, nil
}

// updateTask
func (tm *Manager) updateTask(taskID string, updateTaskInfo *types.SeedTask) (*types.SeedTask, error) {
	if stringutils.IsBlank(taskID) {
		return nil, errors.Wrap(cdnerrors.ErrInvalidValue, "taskID is empty")
	}

	if updateTaskInfo == nil {
		return nil, errors.Wrap(cdnerrors.ErrInvalidValue, "updateTaskInfo is nil")
	}

	if stringutils.IsBlank(updateTaskInfo.CdnStatus) {
		return nil, errors.Wrap(cdnerrors.ErrInvalidValue, "status of task is empty")
	}
	// get origin task
	task, err := tm.getTask(taskID)
	if err != nil {
		return nil, err
	}

	if !updateTaskInfo.IsSuccess() {
		// when the origin CDNStatus equals success, do not update it to unsuccessful
		if task.IsSuccess() {
			return task, nil
		}

		// only update the task CdnStatus when the new task CDNStatus and
		// the origin CDNStatus both not equals success
		task.CdnStatus = updateTaskInfo.CdnStatus
		return task, nil
	}

	// only update the task info when the new CDNStatus equals success
	// and the origin CDNStatus not equals success.
	if updateTaskInfo.CdnFileLength != 0 {
		task.CdnFileLength = updateTaskInfo.CdnFileLength
	}

	if !stringutils.IsBlank(updateTaskInfo.SourceRealDigest) {
		task.SourceRealDigest = updateTaskInfo.SourceRealDigest
	}

	if !stringutils.IsBlank(updateTaskInfo.PieceMd5Sign) {
		task.PieceMd5Sign = updateTaskInfo.PieceMd5Sign
	}
	var pieceTotal int32
	if updateTaskInfo.SourceFileLength > 0 {
		pieceTotal = int32((updateTaskInfo.SourceFileLength + int64(task.PieceSize-1)) / int64(task.PieceSize))
		task.SourceFileLength = updateTaskInfo.SourceFileLength
	}
	if pieceTotal != 0 {
		task.PieceTotal = pieceTotal
	}
	task.CdnStatus = updateTaskInfo.CdnStatus
	return task, nil
}

// isSameTask check whether the two task provided are the same
func isSameTask(task1, task2 *types.SeedTask) bool {
	if task1 == task2 {
		return true
	}
	if task1.TaskURL != task2.TaskURL {
		return false
	}

	if !stringutils.IsBlank(task1.RequestDigest) && !stringutils.IsBlank(task2.RequestDigest) {
		if task1.RequestDigest != task2.RequestDigest {
			return false
		}
	}

	if !stringutils.IsBlank(task1.RequestDigest) && !stringutils.IsBlank(task2.SourceRealDigest) {
		return task1.SourceRealDigest == task2.RequestDigest
	}

	return true
}
