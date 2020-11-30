package localcdn

import (
	"context"
	"fmt"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/source"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/store"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"hash"
)

type cacheDetector struct {
	cacheStore           *store.Store
	metaDataManager      *fileMetaDataManager
	pieceMetaDataManager *pieceMetaDataManager
	sourceClientMgr      *source.Manager
}

// meta info already downloaded
type detectCacheResult struct {
	breakNum         int
	fileLength       int64             // length of file has been downloaded
	pieceMetaRecords []pieceMetaRecord // piece meta data of taskId
	fileMetaData     *fileMetaData     // file meta data of taskId
	fileMd5          hash.Hash         // md5 of content that has been downloaded
}


func newCacheDetector(cacheStore *store.Store, metaDataManager *fileMetaDataManager, pieceMetaDataManager *pieceMetaDataManager, sourceClientMgr *source.Manager) *cacheDetector {
	return &cacheDetector{
		cacheStore:           cacheStore,
		metaDataManager:      metaDataManager,
		pieceMetaDataManager: pieceMetaDataManager,
		sourceClientMgr:      sourceClientMgr,
	}
}

// detectCache detects whether there is a corresponding file in the local.
// If any, check whether the entire file has been completely downloaded.
//
// If so, return the md5 of task file and return startPieceNum as -1.
// And if not, return the latest piece num that has been downloaded.
func (cd *cacheDetector) detectCache(ctx context.Context, task *types.CdnTaskInfo) (*detectCacheResult, error) {
	// read metaData from storage
	detectResult, err := cd.readCache(ctx, task)
	logrus.Infof("taskID: %s detects cache breakNum: %d", task.TaskID, detectResult.breakNum)

	if err != nil || detectResult == nil || detectResult.breakNum == 0 {
		metaData, err := cd.resetRepo(ctx, task)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to reset repo")
		}
		detectResult = &detectCacheResult{fileMetaData: metaData}
	} else {
		logrus.Debugf("taskID: %s start to update access time", task.TaskID)
		if err := cd.metaDataManager.updateAccessTime(ctx, task.TaskID, getCurrentTimeMillisFunc()); err != nil {
			logrus.Warnf( "taskID %s failed to update task access time ", task.TaskID)
		}
	}
	return detectResult, nil
}

func (cd *cacheDetector) readCache(ctx context.Context, task *types.CdnTaskInfo) (result *detectCacheResult, err error) {
	result = &detectCacheResult{}
	// task meta data
	metaData, err := cd.metaDataManager.readFileMetaData(ctx, task.TaskID)
	if err != nil {
		return nil, errors.Wrapf(err, "taskId: %s read file meta data error", task.TaskID)
	}
	if !checkSameFile(task, metaData) {
		return nil, fmt.Errorf("taskID: %s check same file fail", task.TaskID)
	}

	sourceClient, err := cd.sourceClientMgr.Get(task.Url)
	if err != nil {
		return nil, errors.Wrapf(err, "taskID: %s failed to get source client for url(%s)", task.TaskID, task.Url)
	}
	// check source whether is expire
	expired, err := sourceClient.IsExpired(task.Url, task.Headers, metaData.ExpireInfo)
	if err != nil {
		return nil, errors.Wrapf(err, "taskID: %s failed to check whether the task has expired", task.TaskID)
	}

	logrus.Debugf("taskID: %s success to get expired result: %t", task.TaskID, expired)
	if expired {
		return nil, errors.Wrapf(err, "taskID: %s task has expired", task.TaskID)
	}
	// not expired
	if metaData.Finish {
		if metaData.Success {
			pieceMetaRecords, err := cd.pieceMetaDataManager.readAndCheckPieceMetaRecords(ctx, task.TaskID, metaData.Md5)
			if err != nil {
				return result, errors.Wrapf(err, "taskID: %s failed to read piece meta data", task.TaskID)
			}
			result.pieceMetaRecords = pieceMetaRecords
			result.fileMetaData = metaData
			result.fileLength = metaData.CdnFileLength
			result.breakNum = -1
			return result, nil
		}
		return nil, fmt.Errorf("taskID: %s task has downloaded but the result of downloaded is fail", task.TaskID)
	}
	// check source whether support range
	supportRange, err := sourceClient.IsSupportRange(task.Url, task.Headers)
	if err != nil {
		return nil, errors.Wrapf(err, "taskID: %s failed to check whether url %s supports range", task.TaskID, task.Url)
	}
	if !supportRange || task.CdnFileLength < 0 {
		return nil, fmt.Errorf("taskID: %s failed to check whether the task supports partial requests: %v", task.TaskID, err)
	}

	return cd.parseCacheByCheckFile(ctx, task.TaskID, metaData)

}

func (cd *cacheDetector) parseCacheByCheckFile(ctx context.Context, taskID string, metaData *fileMetaData) (result *detectCacheResult, err error) {
	result = &detectCacheResult{}
	cacheReader := newCacheReader()
	reader, err := cd.cacheStore.Get(ctx, getDownloadRawFunc(taskID))
	if err != nil {
		logrus.Errorf("taskID: %s, failed to read key file: %v", taskID, err)
		return nil, err
	}
	pieceMetaRecords, err := cd.pieceMetaDataManager.readWithoutCheckPieceMetaRecords(ctx, taskID)
	if err != nil {
		logrus.Errorf("taskID: %s, failed to read piece meta file: %v", taskID, err)
		return nil, err
	}

	cacheResult, err := cacheReader.readFile(ctx, reader, pieceMetaRecords)
	if err != nil {
		logrus.Errorf("taskID: %s, failed to read data file: %v", taskID, err)
		return nil, err
	}
	result.fileMetaData = metaData
	result.breakNum = cacheResult.breakNum
	result.pieceMetaRecords = cacheResult.pieceMetaRecords
	result.fileLength = cacheResult.fileLength
	result.fileMd5 = cacheResult.fileMd5
	return result, nil
}

func (cd *cacheDetector) resetRepo(ctx context.Context, task *types.CdnTaskInfo) (*fileMetaData, error) {
	logrus.Infof("taskID: %s reset repo for task", task.TaskID)
	if err := deleteTaskFiles(ctx, cd.cacheStore, task.TaskID); err != nil {
		logrus.Errorf("taskID: %s reset repo: failed to delete task files: %v", task.TaskID, err)
	}
	if err := cd.pieceMetaDataManager.removePieceMetaRecordsByTaskID(task.TaskID); err != nil {
		logrus.Errorf("taskID: %s reset repo: failed to remove task files: %v", task.TaskID, err)
	}
	// initialize meta data file
	return cd.metaDataManager.writeFileMetaDataByTask(ctx, task)
}

func checkSameFile(task *types.CdnTaskInfo, metaData *fileMetaData) (result bool) {
	defer func() {
		logrus.Debugf("taskID: %s check same File for task get result: %t", task.TaskID, result)
	}()

	if task == nil || metaData == nil {
		return false
	}

	if metaData.PieceSize != task.PieceSize {
		return false
	}

	if metaData.TaskID != task.TaskID {
		return false
	}

	if metaData.URL != task.Url {
		return false
	}
	if metaData.SourceFileLen != task.SourceFileLength {
		return false
	}
	return true
}
