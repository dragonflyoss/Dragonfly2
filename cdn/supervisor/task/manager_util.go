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
	"time"

	"github.com/pkg/errors"

	cdnerrors "d7y.io/dragonfly/v2/cdn/errors"
	"d7y.io/dragonfly/v2/cdn/types"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
)

// getTaskUnreachableTime get unreachable time of task and convert it to time.Time type
func (tm *Manager) getTaskUnreachableTime(taskID string) (time.Time, bool) {
	unreachableTime, ok := tm.taskURLUnReachableStore.Load(taskID)
	if !ok {
		return time.Time{}, false
	}
	return unreachableTime.(time.Time), true
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

// IsSame check whether the two task provided are the same
func IsSame(task1, task2 *types.SeedTask) bool {
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
