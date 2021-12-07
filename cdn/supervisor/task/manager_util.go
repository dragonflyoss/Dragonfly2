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

	"d7y.io/dragonfly/v2/pkg/util/stringutils"
)

// updateTask updates task
func (tm *manager) updateTask(taskID string, updateTaskInfo *SeedTask) error {
	if updateTaskInfo == nil {
		return errors.New("updateTaskInfo is nil")
	}

	if stringutils.IsBlank(updateTaskInfo.CdnStatus) {
		return errors.New("status of updateTaskInfo is empty")
	}
	// get origin task
	task, ok := tm.getTask(taskID)
	if !ok {
		return errTaskNotFound
	}

	if !updateTaskInfo.IsSuccess() {
		if task.IsSuccess() {
			task.Log().Warnf("origin task status is success, but update task status is %s, return origin task", task.CdnStatus)
			return nil
		}
		task.CdnStatus = updateTaskInfo.CdnStatus
		return nil
	}

	// only update the task info when the updateTaskInfo CDNStatus equals success
	// and the origin CDNStatus not equals success.
	if updateTaskInfo.CdnFileLength > 0 {
		task.CdnFileLength = updateTaskInfo.CdnFileLength
	}
	if !stringutils.IsBlank(updateTaskInfo.SourceRealDigest) {
		task.SourceRealDigest = updateTaskInfo.SourceRealDigest
	}

	if !stringutils.IsBlank(updateTaskInfo.PieceMd5Sign) {
		task.PieceMd5Sign = updateTaskInfo.PieceMd5Sign
	}
	if updateTaskInfo.SourceFileLength >= 0 {
		task.TotalPieceCount = updateTaskInfo.TotalPieceCount
		task.SourceFileLength = updateTaskInfo.SourceFileLength
	}
	task.CdnStatus = updateTaskInfo.CdnStatus
	return nil
}

// getTask get task from taskStore and convert it to *SeedTask type
func (tm *manager) getTask(taskID string) (*SeedTask, bool) {
	task, ok := tm.taskStore.Load(taskID)
	if !ok {
		return nil, false
	}
	return task.(*SeedTask), true
}

// getTaskAccessTime get access time of task and convert it to time.Time type
func (tm *manager) getTaskAccessTime(taskID string) (time.Time, bool) {
	access, ok := tm.accessTimeMap.Load(taskID)
	if !ok {
		return time.Time{}, false
	}
	return access.(time.Time), true
}

// getTaskUnreachableTime get unreachable time of task and convert it to time.Time type
func (tm *manager) getTaskUnreachableTime(taskID string) (time.Time, bool) {
	unreachableTime, ok := tm.taskURLUnreachableStore.Load(taskID)
	if !ok {
		return time.Time{}, false
	}
	return unreachableTime.(time.Time), true
}

// IsSame check if task1 is same with task2
func IsSame(task1, task2 *SeedTask) bool {
	if task1 == task2 {
		return true
	}

	if task1.ID != task2.ID {
		return false
	}

	if task1.TaskURL != task2.TaskURL {
		return false
	}

	if task1.Range != task2.Range {
		return false
	}

	if task1.Tag != task2.Tag {
		return false
	}

	if task1.Digest != task2.Digest {
		return false
	}

	if task1.Filter != task2.Filter {
		return false
	}
	return true
}
