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
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/cdnerrors"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/config"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/util"
	"github.com/dragonflyoss/Dragonfly2/pkg/dferrors"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/netutils"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/stringutils"
	"time"

	"github.com/pkg/errors"
)

// addOrUpdateTask add a new task or update exist task
func (tm *Manager) addOrUpdateTask(ctx context.Context, request *types.TaskRegisterRequest) (*types.SeedTask, error) {
	taskURL := request.URL
	if request.Filter != nil {
		taskURL = netutils.FilterURLParam(request.URL, request.Filter)
	}
	taskId := request.TaskID
	util.GetLock(taskId, false)
	defer util.ReleaseLock(taskId, false)
	if key, err := tm.taskURLUnReachableStore.Get(taskId); err == nil {
		if unReachableStartTime, ok := key.(time.Time); ok &&
			time.Since(unReachableStartTime) < tm.cfg.FailAccessInterval {
			return nil, errors.Wrapf(cdnerrors.ErrURLNotReachable,
				"task hit unReachable cache and interval less than %d, url: %s", tm.cfg.FailAccessInterval, request.URL)
		}
		tm.taskURLUnReachableStore.Delete(taskId)
	}
	var task *types.SeedTask
	newTask := &types.SeedTask{
		TaskID:     taskId,
		Headers:    request.Headers,
		RequestMd5: request.Md5,
		Url:        request.URL,
		TaskUrl:    taskURL,
		CdnStatus:  types.TaskInfoCdnStatusWAITING,
		PieceTotal: -1,
	}
	// using the existing task if it already exists corresponding to taskID
	if v, err := tm.taskStore.Get(taskId); err == nil {
		task = v.(*types.SeedTask)
		if !equalsTask(task, newTask) {
			return nil, cdnerrors.ErrTaskIDDuplicate
		}
	} else {
		task = newTask
	}

	if task.SourceFileLength != 0 {
		return task, nil
	}

	// get sourceContentLength with req.Headers
	sourceFileLength, err := tm.resourceClient.GetContentLength(task.Url, request.Headers)
	if err != nil {
		logger.WithTaskID(task.TaskID).Errorf("failed to get url (%s) content length from http client:%v", task.Url, err)

		if cdnerrors.IsURLNotReachable(err) {
			tm.taskURLUnReachableStore.Add(taskId, time.Now())
			return nil, err
		}
		if cdnerrors.IsAuthenticationRequired(err) {
			return nil, err
		}
	}
	// source cdn
	if tm.cfg.CDNPattern == config.CDNPatternSource {
		if sourceFileLength <= 0 {
			return nil, fmt.Errorf("taskID: %s failed to get file length and it is required in source CDN pattern", task.TaskID)
		}
		// check whether support range
		supportRange, err := tm.resourceClient.IsSupportRange(task.Url, task.Headers)
		if err != nil {
			return nil, errors.Wrapf(err, "taskID: %s failed to check whether the url(%s) supports partial requests", task.TaskID, task.Url)
		}
		if !supportRange {
			return nil, fmt.Errorf("taskID: %s the task URL should support range request in source CDN pattern", task.TaskID)
		}
	}
	// if not support file length header request ,return -1
	task.SourceFileLength = sourceFileLength
	logger.WithTaskID(taskId).Infof("get file content length: %d", sourceFileLength)

	// if success to get the information successfully with the req.Headers then update the task.Headers to req.Headers.
	if request.Headers != nil {
		task.Headers = request.Headers
	}

	// calculate piece size and update the PieceSize and PieceTotal
	pieceSize := computePieceSize(task.SourceFileLength)
	task.PieceSize = pieceSize
	task.PieceTotal = int32((sourceFileLength + (int64(pieceSize) - 1)) / int64(pieceSize))

	tm.taskStore.Add(task.TaskID, task)
	tm.metrics.tasks.WithLabelValues(task.CdnStatus).Inc()
	return task, nil
}

// updateTask
func (tm *Manager) updateTask(taskID string, updateTaskInfo *types.SeedTask) (*types.SeedTask, error) {
	if stringutils.IsEmptyStr(taskID) {
		return nil, errors.Wrap(dferrors.ErrEmptyValue, "taskID")
	}

	if updateTaskInfo == nil {
		return nil, errors.Wrap(dferrors.ErrEmptyValue, "Update TaskInfo")
	}
	// the expected new CDNStatus is not nil
	if stringutils.IsEmptyStr(updateTaskInfo.CdnStatus) {
		return nil, errors.Wrapf(dferrors.ErrEmptyValue, "CDNStatus of TaskInfo: %+v", updateTaskInfo)
	}
	util.GetLock(taskID, false)
	util.ReleaseLock(taskID, false)
	// get origin task
	task, err := tm.getTask(taskID)
	if err != nil {
		return nil, err
	}

	if !isSuccessCDN(updateTaskInfo.CdnStatus) {
		// when the origin CDNStatus equals success, do not update it to unsuccessful
		if isSuccessCDN(task.CdnStatus) {
			return task, nil
		}

		// only update the task CdnStatus when the new task CDNStatus and
		// the origin CDNStatus both not equals success
		tm.metrics.tasks.WithLabelValues(task.CdnStatus).Dec()
		tm.metrics.tasks.WithLabelValues(updateTaskInfo.CdnStatus).Inc()
		task.CdnStatus = updateTaskInfo.CdnStatus
		return task, nil
	}

	// only update the task info when the new CDNStatus equals success
	// and the origin CDNStatus not equals success.
	if updateTaskInfo.CdnFileLength != 0 {
		task.CdnFileLength = updateTaskInfo.CdnFileLength
	}

	if !stringutils.IsEmptyStr(updateTaskInfo.SourceRealMd5) {
		task.SourceRealMd5 = updateTaskInfo.SourceRealMd5
	}

	if !stringutils.IsEmptyStr(updateTaskInfo.PieceMd5Sign) {
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
	tm.metrics.tasks.WithLabelValues(task.CdnStatus).Dec()
	tm.metrics.tasks.WithLabelValues(updateTaskInfo.CdnStatus).Inc()
	task.CdnStatus = updateTaskInfo.CdnStatus
	return task, nil
}

// equalsTask check whether the two task provided are the same
func equalsTask(existTask, newTask *types.SeedTask) bool {
	if existTask.TaskUrl != newTask.TaskUrl {
		return false
	}

	if !stringutils.IsEmptyStr(existTask.RequestMd5) && !stringutils.IsEmptyStr(newTask.RequestMd5) {
		if existTask.RequestMd5 != newTask.RequestMd5 {
			return false
		}
	}

	if !stringutils.IsEmptyStr(newTask.RequestMd5) && !stringutils.IsEmptyStr(existTask.SourceRealMd5) {
		return existTask.SourceRealMd5 == newTask.RequestMd5
	}

	return true
}

// computePieceSize computes the piece size with specified fileLength.
//
// If the fileLength<=0, which means failed to get fileLength
// and then use the DefaultPieceSize.
func computePieceSize(length int64) int32 {
	if length <= 0 || length <= 200*1024*1024 {
		return config.DefaultPieceSize
	}

	gapCount := length / int64(100*1024*1024)
	mpSize := (gapCount-2)*1024*1024 + config.DefaultPieceSize
	if mpSize > config.DefaultPieceSizeLimit {
		return config.DefaultPieceSizeLimit
	}
	return int32(mpSize)
}

// isSuccessCDN determines that whether the CDNStatus is success.
func isSuccessCDN(CDNStatus string) bool {
	return CDNStatus == types.TaskInfoCdnStatusSUCCESS
}

// isFrozen
func isFrozen(CDNStatus string) bool {
	return CDNStatus == types.TaskInfoCdnStatusFAILED ||
		CDNStatus == types.TaskInfoCdnStatusWAITING ||
		CDNStatus == types.TaskInfoCdnStatusSOURCEERROR
}

func isWait(CDNStatus string) bool {
	return CDNStatus == types.TaskInfoCdnStatusWAITING
}

// isErrorCDN
func isErrorCDN(CDNStatus string) bool {
	return CDNStatus == types.TaskInfoCdnStatusFAILED ||
		CDNStatus == types.TaskInfoCdnStatusSOURCEERROR
}
