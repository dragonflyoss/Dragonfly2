package localcdn

import (
	"context"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
	"hash"

	"github.com/dragonflyoss/Dragonfly2/cdnsystem/config"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/store"
	"github.com/dragonflyoss/Dragonfly2/pkg/stringutils"

	"github.com/sirupsen/logrus"
)

type reporter struct {
	cfg *config.Config

	cacheStore           *store.Store
	metaDataManager      *fileMetaDataManager
	pieceMetaDataManager *pieceMetaDataManager
}

func newReporter(cfg *config.Config, cacheStore *store.Store,
	metaDataManager *fileMetaDataManager, pieceMetaDataManager *pieceMetaDataManager) *reporter {
	return &reporter{
		cfg:                  cfg,
		cacheStore:           cacheStore,
		metaDataManager:      metaDataManager,
		pieceMetaDataManager: pieceMetaDataManager,
	}
}

func (re *reporter) reportCache(ctx context.Context, taskID string, detectResult *detectCacheResult) (hash.Hash, *types.SeedTaskInfo, error) {
	// cache not hit
	if detectResult.breakNum == 0 {
		return nil, nil, nil
	}
	// resolve by file meta data
	success, updateTaskInfo, err := re.processFullCache(ctx, taskID, detectResult)
	if err == nil && success {
		// it is possible to succeed only if breakNum equals -1
		return nil, updateTaskInfo, nil
	}
	logrus.Errorf("failed to process cache by quick taskID(%s): %v", taskID, err)

	// If we can't get the information quickly from fileMetaData,
	// and then we have to get that by reading the file.
	return re.processPartialCache(ctx, taskID, detectResult)
}

func (re *reporter) processFullCache(ctx context.Context, taskID string, detectResult *detectCacheResult) (bool, *types.SeedTaskInfo, error) {
	if detectResult.breakNum != -1 {
		logrus.Debugf("taskID: %s failed to processFullCache: breakNum not equals -1 ", taskID)
		return false, nil, nil
	}

	// validate the file md5
	if stringutils.IsEmptyStr(detectResult.fileMetaData.Md5) {
		logrus.Debugf("taskID: %s failed to processFullCache: empty Md5", taskID)
		return false, nil, nil
	}

	// validate the piece meta data
	var err error
	if len(detectResult.pieceMetaRecords) == 0 {
		logrus.Debugf("taskID %s failed to processFullCache: empty pieceMd5s: %v", err)
		return false, nil, nil
	}
	storageInfo, err := re.cacheStore.Stat(ctx, getDownloadRawFunc(taskID))
	if err != nil {
		logrus.Debugf("taskID %s failed to processFullCache: check file size fail: %v", taskID, err)
		return false, nil, nil
	}
	if detectResult.fileMetaData.CdnFileLength != storageInfo.Size {
		logrus.Debugf("taskID %s failed to processFullCache: fileSize not match with file meta data", taskID)
		return false, nil, nil
	}

	return true, getUpdateTaskInfo(types.TaskInfoCdnStatusSUCCESS, detectResult.fileMetaData.Md5, detectResult.fileMetaData.CdnFileLength),
		re.reportPiecesStatus(ctx, taskID, detectResult.pieceMetaRecords)
}

func (re *reporter) processPartialCache(ctx context.Context, taskID string, detectResult *detectCacheResult) (hash.Hash, *types.SeedTaskInfo, error) {

	if err := re.reportPiecesStatus(ctx, taskID, detectResult.pieceMetaRecords); err != nil {
		return nil, nil, err
	}
	return detectResult.fileMd5, nil, nil
}

func (re *reporter) reportPiecesStatus(ctx context.Context, taskID string, pieceMetaRecords []pieceMetaRecord) error {
	// report pieces status

	for pieceNum := 0; pieceNum < len(pieceMetaRecords); pieceNum++ {
		if err := re.reportPieceStatus(ctx, taskID, pieceNum, pieceMetaRecords[pieceNum], config.PieceSUCCESS); err != nil {
			return err
		}
	}
	return nil
}

func (re *reporter) reportPieceStatus(ctx context.Context, taskID string, pieceNum int, pieceMetaRecord pieceMetaRecord, pieceStatus int) (err error) {
	defer func() {
		if err == nil {
			logrus.Debugf("success to report piece status with taskID(%s) pieceNum(%d)", taskID, pieceNum)
		}
	}()

	if pieceStatus == config.PieceSUCCESS {
		if err := re.pieceMetaDataManager.setPieceMetaRecord(taskID, pieceNum, pieceMetaRecord); err != nil {
			return err
		}
	}

	return re.progressManager.UpdateProgress(ctx, taskID, re.cfg.GetCdnCID(taskID), re.cfg.GetSuperPID(), "", pieceNum, pieceStatus)
}
