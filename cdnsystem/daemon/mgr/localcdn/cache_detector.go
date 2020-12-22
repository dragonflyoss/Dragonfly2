package localcdn

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/source"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/store"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
	"github.com/dragonflyoss/Dragonfly2/pkg/errortypes"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/fileutils"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/stringutils"
	"github.com/dragonflyoss/Dragonfly2/pkg/util"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"hash"
	"io"
	"sort"
)

type cacheDetector struct {
	cacheStore           *store.Store
	metaDataManager      *fileMetaDataManager
	pieceMetaDataManager *pieceMetaDataManager
	resourceClient       source.ResourceClient
}

// meta info already downloaded
type detectCacheResult struct {
	breakNum             int32             // break
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

// detectCache
func (cd *cacheDetector) detectCache(ctx context.Context, task *types.SeedTaskInfo) (*detectCacheResult, error) {
	detectResult, err := cd.readCache(ctx, task)
	logrus.Infof("taskID: %s detects cache breakNum: %d", task.TaskID, detectResult.breakNum)

	if err != nil || detectResult == nil || detectResult.breakNum == 0 {
		logrus.Errorf("taskID: %s detect cache fail, detectResult:%v", task.TaskID, detectResult)
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

// readCache detect file metaData and pieces metaData of specific task
func (cd *cacheDetector) readCache(ctx context.Context, task *types.SeedTaskInfo) (result *detectCacheResult, err error) {
	result = &detectCacheResult{}
	// read task meta data
	metaData, err := cd.metaDataManager.readFileMetaData(ctx, task.TaskID)
	if err != nil {
		return &detectCacheResult{}, errors.Wrapf(err, "taskId: %s read file meta data error", task.TaskID)
	}
	if !checkSameFile(task, metaData) {
		return &detectCacheResult{}, fmt.Errorf("taskID: %s check same task fail", task.TaskID)
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
		// 通过元数据信息快速检验数据是否完整
		return cd.parseByReadMetaFile(ctx, task.TaskID, metaData)
	}
	// check source whether support range. if support read data
	supportRange, err := cd.resourceClient.IsSupportRange(task.Url, task.Headers)
	if err != nil {
		return &detectCacheResult{}, errors.Wrapf(err, "taskID: %s failed to check whether url %s supports range", task.TaskID, task.Url)
	}
	if !supportRange || task.CdnFileLength < 0 {
		return &detectCacheResult{}, fmt.Errorf("taskID: %s failed to check whether the task supports partial requests: %v", task.TaskID, err)
	}

	return cd.parseByReadFile(ctx, task.TaskID, metaData)
}

// parseByReadMetaFile detect cache by metaData of file and pieces
func (cd *cacheDetector) parseByReadMetaFile(ctx context.Context, taskID string, metaData *fileMetaData) (result *detectCacheResult, err error) {
	result = &detectCacheResult{}
	if metaData.Success && !stringutils.IsEmptyStr(metaData.SourceMd5) {
		pieceMetaRecords, err := cd.pieceMetaDataManager.getPieceMetaRecordsByTaskID(taskID)
		if err != nil && errortypes.IsDataNotFound(err) {
			// check piece meta records integrity
			pieceMetaRecords, err = cd.pieceMetaDataManager.readAndCheckPieceMetaRecords(ctx, taskID, metaData.SourceMd5)
			if err != nil {
				return &detectCacheResult{}, errors.Wrapf(err, "taskID: %s failed to read piece meta data", taskID)
			}
		}
		// check downloaded data integrity
		storageInfo, err := cd.cacheStore.Stat(ctx, getDownloadRawFunc(taskID))
		if err != nil {
			return &detectCacheResult{}, errors.Wrapf(err, "taskID %s failed to processFullCache: check file size fail", taskID)
		}
		// check downloaded data integrity through file size
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

// parseByReadFile
func (cd *cacheDetector) parseByReadFile(ctx context.Context, taskID string, metaData *fileMetaData) (result *detectCacheResult, err error) {
	result = &detectCacheResult{}
	reader, err := cd.cacheStore.Get(ctx, getDownloadRawFunc(taskID))
	if err != nil {
		return &detectCacheResult{}, errors.Wrapf(err, "taskID: %s, failed to read key file", taskID)
	}
	// read piece records
	pieceMetaRecords, err := cd.pieceMetaDataManager.readWithoutCheckPieceMetaRecords(ctx, taskID)
	if err != nil {
		return &detectCacheResult{}, errors.Wrapf(err, "taskID: %s, failed to read piece meta file", taskID)
	}
	if pieceMetaRecords == nil {
		return &detectCacheResult{}, nil
	}

	fileMd5 := md5.New()
	var downloadedFileLength int64 = 0
	// sort piece meta records by pieceNum
	sort.Slice(pieceMetaRecords, func(i, j int) bool {
		return pieceMetaRecords[i].PieceNum < pieceMetaRecords[j].PieceNum
	})

	var breakIndex int32
	for index, record := range pieceMetaRecords {
		if int32(index) != record.PieceNum {
			breakIndex = int32(index)
			break
		}
		// read content
		if err := readContent(reader, record, fileMd5); err != nil {
			logrus.Errorf("failed to read content for pieceNum %d: %v", record.PieceNum, err)
			breakIndex = int32(index)
			break
		}
		downloadedFileLength += int64(record.PieceLen)
	}
	result.pieceMetaRecords = pieceMetaRecords[0:breakIndex]
	result.breakNum = breakIndex
	result.fileMetaData = metaData
	result.downloadedFileLength = downloadedFileLength
	result.fileMd5 = fileMd5
	return result, nil
}

func (cd *cacheDetector) resetRepo(ctx context.Context, task *types.SeedTaskInfo) (*fileMetaData, error) {
	logrus.Infof("taskID: %s reset repo for task", task.TaskID)
	if err := deleteTaskFiles(ctx, cd.cacheStore, task.TaskID); err != nil {
		logrus.Errorf("taskID: %s reset repo: failed to delete task files: %v", task.TaskID, err)
	}
	if err := cd.pieceMetaDataManager.removePieceMetaRecordsByTaskID(task.TaskID); err != nil && !errortypes.IsDataNotFound(err){
		logrus.Errorf("taskID: %s reset repo: failed to remove task files: %v", task.TaskID, err)
	}
	// initialize meta data file
	return cd.metaDataManager.writeFileMetaDataByTask(ctx, task)
}

// checkSameFile check whether meta file is modified
func checkSameFile(task *types.SeedTaskInfo, metaData *fileMetaData) (result bool) {
	defer func() {
		logrus.Debugf("taskID: %s check same task get result: %t", task.TaskID, result)
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
	if !stringutils.IsEmptyStr(metaData.SourceMd5) && !stringutils.IsEmptyStr(task.RequestMd5) {
		if metaData.SourceMd5 != task.RequestMd5 {
			return false
		}
	}
	return true
}

//readContent read piece content
func readContent(reader io.Reader, pieceMetaRecord pieceMetaRecord, fileMd5 hash.Hash) error {
	bufSize := int32(256 * 1024)
	pieceLen := pieceMetaRecord.PieceLen
	if pieceLen < bufSize {
		bufSize = pieceLen
	}
	pieceContent := make([]byte, bufSize)
	var curContent int32
	pieceMd5 := md5.New()
	for {
		if curContent+bufSize <= pieceLen {
			if err := binary.Read(reader, binary.BigEndian, pieceContent); err != nil {
				return err
			}
			curContent += bufSize
			// calculate the md5
			// todo 应该存放原始文件的md5
			pieceMd5.Write(pieceContent)

			if !util.IsNil(fileMd5) {
				fileMd5.Write(pieceContent)
			}
		} else {
			readLen := pieceLen - curContent
			if err := binary.Read(reader, binary.BigEndian, pieceContent[:readLen]); err != nil {
				return err
			}
			curContent += readLen
			// calculate the md5
			pieceMd5.Write(pieceContent[:readLen])
			if !util.IsNil(fileMd5) {
				fileMd5.Write(pieceContent[:readLen])
			}
		}
		if curContent >= pieceLen {
			break
		}
	}
	realMd5 := fileutils.GetMd5Sum(pieceMd5, nil)
	// check piece content integrity
	if realMd5 != pieceMetaRecord.Md5 {
		return fmt.Errorf("piece md5 check fail, read md5 (%s), expected md5 (%s)", realMd5, pieceMetaRecord.Md5)
	}
	return nil
}
