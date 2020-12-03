package task

import (
	"context"
	"fmt"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/config"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/util"
	"github.com/dragonflyoss/Dragonfly2/pkg/errortypes"
	"github.com/dragonflyoss/Dragonfly2/pkg/stringutils"
	"github.com/sirupsen/logrus"
	"time"

	"github.com/pkg/errors"
)


func (tm *Manager) addOrUpdateTask(ctx context.Context, request *types.TaskRegisterRequest) (*types.SeedTaskInfo, error) {
	taskId := request.TaskID
	util.GetLock(taskId, false)
	defer util.ReleaseLock(taskId, false)
	if key, err := tm.taskURLUnReachableStore.Get(taskId); err == nil {
		if unReachableStartTime, ok := key.(time.Time); ok &&
			time.Since(unReachableStartTime) < tm.cfg.FailAccessInterval {
			return nil, errors.Wrapf(errortypes.ErrURLNotReachable, "taskID: %s task hit unReachable cache and interval less than %d, url: %s", taskId, tm.cfg.FailAccessInterval, request.URL)
		}
		tm.taskURLUnReachableStore.Delete(taskId)
	}
	var task *types.SeedTaskInfo
	newTask := &types.SeedTaskInfo{
		TaskID:     taskId,
		Headers:    request.Headers,
		RequestMd5: request.Md5,
		Url:        request.URL,
		CdnStatus:  types.TaskInfoCdnStatusWAITING,
		PieceTotal: -1,
	}
	// using the existing task if it already exists corresponding to taskID
	if v, err := tm.taskStore.Get(taskId); err == nil {
		task = v.(*types.SeedTaskInfo)
		if !equalsTask(task, newTask) {
			return nil, errors.Wrapf(errortypes.ErrTaskIDDuplicate, "%s", task.TaskID)
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
		logrus.Errorf("taskID: %s failed to get url (%s) file length from http client : %v", task.TaskID, task.Url, err)

		if errortypes.IsURLNotReachable(err) {
			tm.taskURLUnReachableStore.Add(taskId, time.Now())
			return nil, err
		}
		if errortypes.IsAuthenticationRequired(err) {
			return nil, err
		}
	}
	// source cdn
	if tm.cfg.CDNPattern == config.CDNPatternSource {
		if sourceFileLength <= 0 {
			return nil, fmt.Errorf("taskID: %s failed to get file length and it is required in source CDN pattern", task.TaskID)
		}

		supportRange, err := tm.resourceClient.IsSupportRange(task.Url, task.Headers)
		if err != nil {
			return nil, errors.Wrapf(err, "taskID: %s failed to check whether the task supports partial requests", task.TaskID)
		}
		if !supportRange {
			return nil, fmt.Errorf("taskID: %s the task URL should support range request in source CDN pattern", task.TaskID)
		}
	}
	// if not support file length header request ,return -1
	task.SourceFileLength = sourceFileLength
	logrus.Infof("taskID: %s get file length %d from http client for task", task.TaskID, sourceFileLength)

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

func (tm *Manager) updateTask(taskID string, updateTaskInfo *types.SeedTaskInfo) error {
	if stringutils.IsEmptyStr(taskID) {
		return errors.Wrap(errortypes.ErrEmptyValue, "taskID")
	}

	if updateTaskInfo == nil {
		return errors.Wrap(errortypes.ErrEmptyValue, "Update TaskInfo")
	}

	// the expected new CDNStatus is not nil
	if stringutils.IsEmptyStr(updateTaskInfo.CdnStatus) {
		return errors.Wrapf(errortypes.ErrEmptyValue, "CDNStatus of TaskInfo: %+v", updateTaskInfo)
	}

	task, err := tm.getTask(taskID)
	if err != nil {
		return err
	}

	if !isSuccessCDN(updateTaskInfo.CdnStatus) {
		// when the origin CDNStatus equals success, do not update it to unsuccessful
		if isSuccessCDN(task.CdnStatus) {
			return nil
		}

		// only update the task CdnStatus when the new CDNStatus and
		// the origin CDNStatus both not equals success
		tm.metrics.tasks.WithLabelValues(task.CdnStatus).Dec()
		tm.metrics.tasks.WithLabelValues(updateTaskInfo.CdnStatus).Inc()
		task.CdnStatus = updateTaskInfo.CdnStatus
		return nil
	}

	// only update the task info when the new CDNStatus equals success
	// and the origin CDNStatus not equals success.
	if updateTaskInfo.CdnFileLength != 0 {
		task.CdnFileLength = updateTaskInfo.CdnFileLength
	}

	if !stringutils.IsEmptyStr(updateTaskInfo.RealMd5) {
		task.RealMd5 = updateTaskInfo.RealMd5
	}

	var pieceTotal int32
	if updateTaskInfo.SourceFileLength > 0 {
		pieceTotal = int32((updateTaskInfo.SourceFileLength + int64(task.PieceSize-1)) / int64(task.PieceSize))
	}
	if pieceTotal != 0 {
		task.PieceTotal = pieceTotal
	}
	tm.metrics.tasks.WithLabelValues(task.CdnStatus).Dec()
	tm.metrics.tasks.WithLabelValues(updateTaskInfo.CdnStatus).Inc()
	task.CdnStatus = updateTaskInfo.CdnStatus

	return nil
}

// equalsTask check whether the two task provided are the same
// The result is based only on whether the attributes used to generate taskID are the same
// which including taskURL, md5, identifier.
func equalsTask(existTask, newTask *types.SeedTaskInfo) bool {
	if existTask.Url != newTask.Url {
		return false
	}

	if !stringutils.IsEmptyStr(existTask.RequestMd5) {
		return existTask.RequestMd5 == newTask.RequestMd5
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

func isFrozen(CDNStatus string) bool {
	return CDNStatus == types.TaskInfoCdnStatusFAILED ||
		CDNStatus == types.TaskInfoCdnStatusWAITING ||
		CDNStatus == types.TaskInfoCdnStatusSOURCEERROR
}
