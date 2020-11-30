package localcdn

import (
	"context"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
	pb "github.com/dragonflyoss/Dragonfly2/pkg/grpc/cdnsystem"
	"hash"

	"github.com/dragonflyoss/Dragonfly2/cdnsystem/config"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/store"
	"github.com/dragonflyoss/Dragonfly2/pkg/fileutils"
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

func (re *reporter) reportCache(ctx context.Context, taskID string, detectResult *detectCacheResult) (hash.Hash, *types.CdnTaskInfo, error) {
	// cache not hit
	if detectResult.breakNum == 0 {
		return nil, nil, nil
	}
	// resolve by file meta data
	success, updateTaskInfo, err := re.processCacheByQuick(ctx, taskID, detectResult)
	if err == nil && success {
		// it is possible to succeed only if breakNum equals -1
		return nil, updateTaskInfo, nil
	}
	logrus.Errorf("failed to process cache by quick taskID(%s): %v", taskID, err)

	// If we can't get the information quickly from fileMetaData,
	// and then we have to get that by reading the file.
	return re.processCacheByReadFile(ctx, taskID, detectResult)
}

func (re *reporter) processCacheByQuick(ctx context.Context, taskID string, detectResult *detectCacheResult) (bool, *types.CdnTaskInfo, error) {
	if detectResult.breakNum != -1 {
		logrus.Debugf("taskID: %s failed to processCacheByQuick: breakNum not equals -1 ", taskID)
		return false, nil, nil
	}

	// validate the file md5
	if stringutils.IsEmptyStr(detectResult.fileMetaData.Md5) {
		logrus.Debugf("taskID: %s failed to processCacheByQuick: empty Md5", taskID)
		return false, nil, nil
	}

	// validate the piece meta data
	var err error
	if len(detectResult.pieceMetaRecords) == 0 {
		logrus.Debugf("taskID %s failed to processCacheByQuick: empty pieceMd5s: %v", err)
		return false, nil, nil
	}
	storageInfo, err := re.cacheStore.Stat(ctx, getDownloadRawFunc(taskID))
	if err != nil {
		logrus.Debugf("taskID %s failed to processCacheByQuick: check file size fail: %v", taskID, err)
		return false, nil, nil
	}
	if detectResult.fileMetaData.CdnFileLength != storageInfo.Size {
		logrus.Debugf("taskID %s failed to processCacheByQuick: fileSize not match with file meta data", taskID)
		return false, nil, nil
	}

	return true, getUpdateTaskInfo(types.TaskInfoCdnStatusSUCCESS, detectResult.fileMetaData.Md5, detectResult.fileMetaData.CdnFileLength),
		re.reportPiecesStatus(ctx, taskID, detectResult.pieceMetaRecords)
}

func (re *reporter) processCacheByReadFile(ctx context.Context, taskID string, detectResult *detectCacheResult) (hash.Hash, *types.CdnTaskInfo, error) {
	// does file md5 need to be calculated
	var calculateFileMd5 = true
	// pieces meta valid fail
	if detectResult.breakNum == -1 && !stringutils.IsEmptyStr(detectResult.fileMetaData.Md5) {
		calculateFileMd5 = false
	}

	logrus.Infof("taskID: %s success to get cache result: %+v by read file", taskID, result)

	if err := re.reportPiecesStatus(ctx, taskID, detectResult.pieceMetaRecords); err != nil {
		return nil, nil, err
	}

	if breakNum != -1 {
		return result.fileMd5, nil, nil
	}

	fileMd5Value := metaData.Md5
	if stringutils.IsEmptyStr(fileMd5Value) {
		fileMd5Value = fileutils.GetMd5Sum(result.fileMd5, nil)
	}

	fmd := &fileMetaData{
		Finish:        true,
		Success:       true,
		Md5:           fileMd5Value,
		CdnFileLength: result.fileLength,
	}
	if err := re.metaDataManager.updateStatusAndResult(ctx, taskID, fmd); err != nil {
		logrus.Errorf("failed to update status and result fileMetaData(%+v) for taskID(%s): %v", fmd, taskID, err)
		return nil, nil, err
	}
	logrus.Infof("success to update status and result fileMetaData(%+v) for taskID(%s)", fmd, taskID)

	return nil, getUpdateTaskInfo(types.TaskInfoCdnStatusSUCCESS, fileMd5Value, result.fileLength),
		re.metaDataManager.writePieceMD5s(ctx, taskID, fileMd5Value, result.pieceMd5s)
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
		if err := re.pieceMD5Manager.setPieceMD5(taskID, pieceNum, md5); err != nil {
			return err
		}
	}
	pb.PieceSeed{
		State:         nil,
		SeedAddr:      "",
		PieceStyle:    "",
		PieceNum:      pieceNum,
		PieceMd5:      md5,
		PieceRange:    "",
		PieceOffset:   0,
		Last:          false,
		ContentLength: 0,
	}

	return re.progressManager.UpdateProgress(ctx, taskID, re.cfg.GetCdnCID(taskID), re.cfg.GetSuperPID(), "", pieceNum, pieceStatus)
}
