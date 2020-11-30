package task

import (
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/config"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
	"github.com/dragonflyoss/Dragonfly2/pkg/errortypes"
	"github.com/dragonflyoss/Dragonfly2/pkg/stringutils"

	"github.com/pkg/errors"
)

func (m *Manager) updateTask(taskID string, updateTaskInfo *types.CdnTaskInfo) error {
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

	task, err := m.getTask(taskID)
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
		m.metrics.tasks.WithLabelValues(task.CdnStatus).Dec()
		m.metrics.tasks.WithLabelValues(updateTaskInfo.CdnStatus).Inc()
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
	if updateTaskInfo.CdnFileLength > 0 {
		pieceTotal = int32((updateTaskInfo.CdnFileLength + int64(task.PieceSize-1)) / int64(task.PieceSize))
	}
	if pieceTotal != 0 {
		task.PieceTotal = pieceTotal
	}
	m.metrics.tasks.WithLabelValues(task.CdnStatus).Dec()
	m.metrics.tasks.WithLabelValues(updateTaskInfo.CdnStatus).Inc()
	task.CdnStatus = updateTaskInfo.CdnStatus

	return nil
}

// equalsTask check whether the two task provided are the same
// The result is based only on whether the attributes used to generate taskID are the same
// which including taskURL, md5, identifier.
func equalsTask(existTask, newTask *types.CdnTaskInfo) bool {
	if existTask.Url != newTask.Url {
		return false
	}

	if !stringutils.IsEmptyStr(existTask.Md5) {
		return existTask.Md5 == newTask.Md5
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

func isWait(CDNStatus string) bool {
	return CDNStatus == types.TaskInfoCdnStatusWAITING
}
