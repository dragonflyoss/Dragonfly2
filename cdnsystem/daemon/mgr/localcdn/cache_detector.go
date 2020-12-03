package localcdn

import (
	"context"
	"fmt"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/source"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/store"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
	"github.com/dragonflyoss/Dragonfly2/pkg/stringutils"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"hash"
)

type cacheDetector struct {
	cacheStore           *store.Store
	metaDataManager      *fileMetaDataManager
	pieceMetaDataManager *pieceMetaDataManager
	resourceClient       source.ResourceClient
}

// meta info already downloaded
type detectCacheResult struct {
	breakNum             int32
	downloadedFileLength int64             // length of file has been downloaded
	pieceMetaRecords     []pieceMetaRecord // piece meta data of taskId
	fileMetaData         *fileMetaData     // file meta data of taskId
	fileMd5              hash.Hash         // md5 of content that has been downloaded
}

func newCacheDetector(cacheStore *store.Store, metaDataManager *fileMetaDataManager, pieceMetaDataManager *pieceMetaDataManager,
	resourceClient source.ResourceClient) *cacheDetector {
	return &cacheDetector{
		cacheStore:           cacheStore,
		metaDataManager:      metaDataManager,
		pieceMetaDataManager: pieceMetaDataManager,
		resourceClient:       resourceClient,
	}
}

// detectCache detects whether there is a corresponding file in the local.
// If any, check whether the entire file has been completely downloaded.
//
// If so, return the md5 of task file and return startPieceNum as -1.
// And if not, return the latest piece num that has been downloaded.
func (cd *cacheDetector) detectCache(ctx context.Context, task *types.SeedTaskInfo) (*detectCacheResult, error) {
	// read metaData from storage
	detectResult, err := cd.readCache(ctx, task)
	logrus.Infof("taskID: %s detects cache breakNum: %d", task.TaskID, detectResult.breakNum)

	if err != nil || detectResult == nil || detectResult.breakNum == 0 {
		metaData, err := cd.resetRepo(ctx, task)
		if err != nil {
			return &detectCacheResult{}, errors.Wrapf(err, "failed to reset repo")
		}
		detectResult = &detectCacheResult{fileMetaData: metaData}
	} else {
		logrus.Debugf("taskID: %s start to update access time", task.TaskID)
		if err := cd.metaDataManager.updateAccessTime(ctx, task.TaskID, getCurrentTimeMillisFunc()); err != nil {
			logrus.Warnf("taskID %s failed to update task access time ", task.TaskID)
		}
	}
	return detectResult, nil
}

func (cd *cacheDetector) readCache(ctx context.Context, task *types.SeedTaskInfo) (result *detectCacheResult, err error) {
	result = &detectCacheResult{}
	// read task meta data
	metaData, err := cd.metaDataManager.readFileMetaData(ctx, task.TaskID)
	if err != nil {
		return &detectCacheResult{}, errors.Wrapf(err, "taskId: %s read file meta data error", task.TaskID)
	}
	if !checkSameFile(task, metaData) {
		return &detectCacheResult{}, fmt.Errorf("taskID: %s check same file fail", task.TaskID)
	}
	expired, err := cd.resourceClient.IsExpired(task.Url, task.Headers, metaData.ExpireInfo)
	if err != nil {
		return &detectCacheResult{}, errors.Wrapf(err, "taskID: %s failed to check whether the task has expired", task.TaskID)
	}

	logrus.Debugf("taskID: %s success to get expired result: %t", task.TaskID, expired)
	if expired {
		return &detectCacheResult{}, errors.Wrapf(err, "taskID: %s task has expired", task.TaskID)
	}
	// not expired
	if metaData.Finish {
		return cd.parseCacheByMetaFile(ctx, task.TaskID, metaData)
	}
	// check source whether support range
	supportRange, err := cd.resourceClient.IsSupportRange(task.Url, task.Headers)
	if err != nil {
		return &detectCacheResult{}, errors.Wrapf(err, "taskID: %s failed to check whether url %s supports range", task.TaskID, task.Url)
	}
	if !supportRange || task.CdnFileLength < 0 {
		return &detectCacheResult{}, fmt.Errorf("taskID: %s failed to check whether the task supports partial requests: %v", task.TaskID, err)
	}

	return cd.parseCacheByCheckFile(ctx, task.TaskID, metaData)
}
func (cd *cacheDetector) parseCacheByMetaFile(ctx context.Context, taskID string, metaData *fileMetaData) (result *detectCacheResult, err error) {
	result = &detectCacheResult{}
	if metaData.Success && !stringutils.IsEmptyStr(metaData.Md5) {
		// check piece meta integrity
		pieceMetaRecords, err := cd.pieceMetaDataManager.readAndCheckPieceMetaRecords(ctx, taskID, metaData.Md5)
		if err != nil {
			return &detectCacheResult{}, errors.Wrapf(err, "taskID: %s failed to read piece meta data", taskID)
		}
		// check downloaded data integrity
		storageInfo, err := cd.cacheStore.Stat(ctx, getDownloadRawFunc(taskID))
		if err != nil {
			return &detectCacheResult{}, errors.Wrapf(err, "taskID %s failed to processFullCache: check file size fail", taskID)
		}
		if metaData.CdnFileLength != storageInfo.Size {
			return &detectCacheResult{}, fmt.Errorf("taskID %s failed to processFullCache: fileSize not match with file meta data", taskID)
		}
		result.fileMetaData = metaData
		result.breakNum = -1
		result.pieceMetaRecords = pieceMetaRecords
		result.downloadedFileLength = metaData.CdnFileLength
		return result, nil
	}
	return &detectCacheResult{}, fmt.Errorf("taskID: %s task has downloaded but the result of downloaded is fail", taskID)
}

func (cd *cacheDetector) parseCacheByCheckFile(ctx context.Context, taskID string, metaData *fileMetaData) (result *detectCacheResult, err error) {
	result = &detectCacheResult{}
	cacheReader := newCacheReader()
	reader, err := cd.cacheStore.Get(ctx, getDownloadRawFunc(taskID))
	if err != nil {
		return &detectCacheResult{}, errors.Wrapf(err, "taskID: %s, failed to read key file", taskID)
	}
	pieceMetaRecords, err := cd.pieceMetaDataManager.readWithoutCheckPieceMetaRecords(ctx, taskID)
	if err != nil {
		return &detectCacheResult{}, errors.Wrapf(err, "taskID: %s, failed to read piece meta file", taskID)
	}
	if pieceMetaRecords == nil {
		return &detectCacheResult{}, nil
	}
	cacheResult, err := cacheReader.readFile(reader, pieceMetaRecords)
	if err != nil {
		return &detectCacheResult{}, errors.Wrapf(err, "taskID: %s, failed to read data file")
	}
	result.fileMetaData = metaData
	result.breakNum = cacheResult.breakNum
	result.pieceMetaRecords = cacheResult.pieceMetaRecords
	result.downloadedFileLength = cacheResult.fileLength
	result.fileMd5 = cacheResult.fileMd5
	return result, nil
}

func (cd *cacheDetector) resetRepo(ctx context.Context, task *types.SeedTaskInfo) (*fileMetaData, error) {
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

func checkSameFile(task *types.SeedTaskInfo, metaData *fileMetaData) (result bool) {
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
	if !stringutils.IsEmptyStr(metaData.Md5) && !stringutils.IsEmptyStr(task.RequestMd5) {
		if metaData.Md5 != task.RequestMd5 {
			return false
		}
	}
	return true
}
