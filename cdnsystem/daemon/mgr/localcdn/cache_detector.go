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

package localcdn

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/source"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/store"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
	"github.com/dragonflyoss/Dragonfly2/pkg/dferrors"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
	"github.com/dragonflyoss/Dragonfly2/pkg/util"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/fileutils"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/stringutils"
	"github.com/pkg/errors"
	"hash"
	"io"
	"sort"
)

type cacheDetector struct {
	cacheStore           *store.Store
	metaDataManager      *metaDataManager
	pieceMetaDataManager *SeedPieceMetaDataManager
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

func newCacheDetector(cacheStore *store.Store, metaDataManager *metaDataManager, pieceMetaDataManager *SeedPieceMetaDataManager,
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
	detectResult := cd.parseCache(ctx, task)
	logger.Infof("taskID: %s detects cache result:%v", task.TaskID, detectResult)

	if detectResult == nil || detectResult.breakNum == 0 {
		logger.Errorf("taskID: %s detect cache fail, detectResult:%v", task.TaskID, detectResult)
		metaData, err := cd.resetRepo(ctx, task)
		if err != nil {
			return &detectCacheResult{}, errors.Wrapf(err, "failed to reset repo")
		}
		detectResult = &detectCacheResult{fileMetaData: metaData}
	} else {
		logger.Debugf("taskID: %s start to update access time", task.TaskID)
		if err := cd.metaDataManager.updateAccessTime(ctx, task.TaskID, getCurrentTimeMillisFunc()); err != nil {
			logger.Warnf("taskID %s failed to update task access time ", task.TaskID)
		}
	}
	return detectResult, nil
}

// parseCache detect file metaData and pieces metaData of specific task
func (cd *cacheDetector) parseCache(ctx context.Context, task *types.SeedTaskInfo) *detectCacheResult {
	// read task meta data
	fileMetaData, err := cd.metaDataManager.readFileMetaData(ctx, task.TaskID)
	if err != nil {
		logger.Errorf("taskId: %s read file meta data error:%v", task.TaskID, err)
		return nil
	}
	if !checkSameFile(task, fileMetaData) {
		logger.Errorf("taskID: %s check same file fail", task.TaskID)
		return nil
	}
	expired, err := cd.resourceClient.IsExpired(task.Url, task.Headers, fileMetaData.ExpireInfo)
	if err != nil {
		logger.Errorf("taskID: %s failed to check whether the task has expired:%v", task.TaskID, err)
		return nil
	}
	logger.Debugf("taskID: %s success to get expired result: %t", task.TaskID, expired)
	if expired {
		logger.Errorf("taskID: %s task has expired", task.TaskID)
		return nil
	}
	// not expired
	if fileMetaData.Finish {
		// check data integrity quickly by meta data
		return cd.parseByReadMetaFile(ctx, task.TaskID, fileMetaData)
	}
	// check source whether support range. if support, check cache by reading file data
	supportRange, err := cd.resourceClient.IsSupportRange(task.Url, task.Headers)
	if err != nil {
		logger.Errorf("taskID: %s failed to check whether url %s supports range:%v", task.TaskID, task.Url, err)
		return nil
	}
	if !supportRange {
		logger.Errorf("taskID: %s taskUrl:%s not supports partial requests: %v", task.TaskID, task.Url, err)
		return nil

	}
	return cd.parseByReadFile(ctx, task.TaskID, fileMetaData)
}

// parseByReadMetaFile detect cache by metaData of file and pieces
func (cd *cacheDetector) parseByReadMetaFile(ctx context.Context, taskID string, fileMetaData *fileMetaData) *detectCacheResult {
	if !fileMetaData.Success {
		logger.Errorf("taskID: %s task has downloaded but the result of downloaded is fail", taskID)
		return nil
	}
	var pieceMetaRecords []pieceMetaRecord
	if pieceMetaRecords, err := cd.pieceMetaDataManager.getPieceMetaRecordsByTaskID(taskID); err != nil {
		logger.Warnf("taskID: %s failed to read piece meta data from memory:%v, try read data from disk", taskID, err)
		// check piece meta records integrity
		pieceMetaRecords, err = cd.metaDataManager.readAndCheckPieceMetaRecords(ctx, taskID, fileMetaData.SourceMd5)
		if err != nil {
			logger.Errorf("taskID: %s failed to read piece meta data from disk:%v", taskID, err)
			return nil
		}
		for _, pieceMetaRecord := range pieceMetaRecords {
			cd.pieceMetaDataManager.setPieceMetaRecord(taskID, pieceMetaRecord)
		}
	}
	if len(pieceMetaRecords) != fileMetaData.TotalPieceCount {
		logger.Errorf("taskID: %s piece count not equal with meta piece count", taskID)
		return nil
	}
	// check downloaded data integrity
	storageInfo, err := cd.cacheStore.Stat(ctx, getDownloadRawFunc(taskID))
	if err != nil {
		logger.Errorf("taskID %s failed to processFullCache: check file size fail:%v", taskID, err)
		return nil
	}
	// check downloaded data integrity through file size
	if fileMetaData.CdnFileLength != storageInfo.Size {
		logger.Errorf("taskID %s parseByReadMetaFile fail: fileSize not match with file meta data", taskID)
		return nil
	}

	return &detectCacheResult{
		breakNum:             -1,
		downloadedFileLength: fileMetaData.CdnFileLength,
		pieceMetaRecords:     pieceMetaRecords,
		fileMetaData:         fileMetaData,
		fileMd5:              nil,
	}
}

// parseByReadFile
func (cd *cacheDetector) parseByReadFile(ctx context.Context, taskID string, metaData *fileMetaData) *detectCacheResult {
	reader, err := cd.cacheStore.Get(ctx, getDownloadRawFunc(taskID))
	if err != nil {
		logger.Errorf("taskID: %s, failed to read key file:%v", taskID, err)
		return nil
	}
	// read piece records
	pieceMetaRecords, err := cd.metaDataManager.readWithoutCheckPieceMetaRecords(ctx, taskID)
	if err != nil {
		logger.Errorf("taskID: %s, failed to read piece meta file:%v", taskID, err)
		return nil
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
			logger.Errorf("taskID:%s, failed to read content for pieceNum %d: %v", taskID, record.PieceNum, err)
			breakIndex = int32(index)
			break
		}
		downloadedFileLength += int64(record.PieceLen)
	}
	return &detectCacheResult{
		breakNum:             breakIndex,
		downloadedFileLength: downloadedFileLength,
		pieceMetaRecords:     pieceMetaRecords[0:breakIndex],
		fileMetaData:         metaData,
		fileMd5:              fileMd5,
	}
}

func (cd *cacheDetector) resetRepo(ctx context.Context, task *types.SeedTaskInfo) (*fileMetaData, error) {
	logger.Infof("taskID: %s reset repo for task", task.TaskID)
	if err := deleteTaskFiles(ctx, cd.cacheStore, task.TaskID); err != nil {
		logger.Errorf("taskID: %s reset repo: failed to delete task files: %v", task.TaskID, err)
	}
	if err := cd.pieceMetaDataManager.removePieceMetaRecordsByTaskID(task.TaskID); err != nil && !dferrors.IsDataNotFound(err) {
		logger.Errorf("taskID: %s reset repo: failed to remove task files: %v", task.TaskID, err)
	}
	// initialize meta data file
	return cd.metaDataManager.writeFileMetaDataByTask(ctx, task)
}

// checkSameFile check whether meta file is modified
func checkSameFile(task *types.SeedTaskInfo, metaData *fileMetaData) (result bool) {
	defer func() {
		logger.Debugf("taskID: %s check same task get result: %t", task.TaskID, result)
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
			pieceMd5.Write(pieceContent)

			if !util.IsNil(fileMd5) {
				// todo 应该存放原始文件的md5
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
				// todo 应该存放原始文件的md5
				fileMd5.Write(pieceContent[:readLen])
			}
		}
		if curContent >= pieceLen {
			break
		}
	}
	realPieceMd5 := fileutils.GetMd5Sum(pieceMd5, nil)
	// check piece content integrity
	if realPieceMd5 != pieceMetaRecord.Md5 {
		return errors.Errorf("piece md5 check fail, realPieceMd5 md5 (%s), expected md5 (%s)", realPieceMd5, pieceMetaRecord.Md5)
	}
	return nil
}
