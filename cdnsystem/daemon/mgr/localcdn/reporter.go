package localcdn

import (
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
	"github.com/sirupsen/logrus"
)

type reporter struct {
	pieceMetaDataManager *pieceMetaDataManager
}

func newReporter(pieceMetaDataManager *pieceMetaDataManager) *reporter {
	return &reporter{
		pieceMetaDataManager: pieceMetaDataManager,
	}
}

func (re *reporter) reportCache(taskID string, detectResult *detectCacheResult) (*types.SeedTaskInfo, error) {
	// report cache pieces status
	if detectResult != nil && detectResult.pieceMetaRecords != nil {
		for pieceNum, pieceMetaRecord := range detectResult.pieceMetaRecords {
			if err := re.pieceMetaDataManager.setPieceMetaRecord(taskID, int32(pieceNum), pieceMetaRecord); err != nil {
				logrus.Errorf("taskId: %s failed to report piece meta record pieceNum %d: %v", taskID, pieceNum, err)
			}
		}
	}
	// full cache, update task status
	if detectResult != nil && detectResult.breakNum == -1 {
		return getUpdateTaskInfo(types.TaskInfoCdnStatusSUCCESS, detectResult.fileMetaData.SourceMd5, detectResult.fileMetaData.CdnFileLength), nil
	}
	// partial cache
	return nil, nil
}
