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

package cdn

import (
	"context"
	"crypto/md5"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/source"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/store"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
	"github.com/dragonflyoss/Dragonfly2/pkg/dferrors"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/stringutils"
	"github.com/pkg/errors"
	"hash"
	"sort"
)

// cacheDetector detect cache
type cacheDetector struct {
	cacheStore      *store.Store
	metaDataManager *metaDataManager
	resourceClient  source.ResourceClient
}

// cacheResult detect cache result
type cacheResult struct {
	breakNum             int32             // break num
	downloadedFileLength int64             // length of file has been downloaded
	pieceMetaRecords     []pieceMetaRecord // piece meta data records of task
	fileMetaData         *fileMetaData     // file meta data of task
	fileMd5              hash.Hash         // md5 of file content that has been downloaded
}

func newCacheDetector(cacheStore *store.Store, metaDataManager *metaDataManager,
	resourceClient source.ResourceClient) *cacheDetector {
	return &cacheDetector{
		cacheStore:      cacheStore,
		metaDataManager: metaDataManager,
		resourceClient:  resourceClient,
	}
}

// detectCache detect cache
func (cd *cacheDetector) detectCache(ctx context.Context, task *types.SeedTask) (*cacheResult, error) {
	detectResult, err := cd.doDetect(ctx, task)
	logger.Debugf("taskID: %s detects cache result:%v", task.TaskID, detectResult)
	if err != nil {
		logger.Errorf("taskID:%s, detect cache fail:%v", task.TaskID, err)
		metaData, err := cd.resetRepo(ctx, task)
		if err != nil {
			return &cacheResult{}, errors.Wrapf(err, "failed to reset repo")
		}
		detectResult = &cacheResult{fileMetaData: metaData}
	} else {
		logger.Debugf("taskID: %s start to update access time", task.TaskID)
		if err := cd.metaDataManager.updateAccessTime(ctx, task.TaskID, getCurrentTimeMillisFunc()); err != nil {
			logger.Warnf("taskID %s failed to update task access time ", task.TaskID)
		}
	}
	return detectResult, nil
}

// doDetect detect file metaData and pieces metaData of specific task
func (cd *cacheDetector) doDetect(ctx context.Context, task *types.SeedTask) (*cacheResult, error) {
	// read task meta data
	fileMetaData, err := cd.metaDataManager.readFileMetaData(ctx, task.TaskID)
	if err != nil {
		return &cacheResult{}, errors.Wrapf(err, "read file meta data fail from disk")
	}
	if err := checkSameFile(task, fileMetaData); err != nil {
		return &cacheResult{}, errors.Wrapf(err, "check same file fail")
	}
	expired, err := cd.resourceClient.IsExpired(task.Url, task.Headers, fileMetaData.ExpireInfo)
	if err != nil {
		return &cacheResult{}, errors.Wrapf(err, "check whether the task expired fail")
	}
	logger.Debugf("taskID: %s success to get expired result: %t", task.TaskID, expired)
	if expired {
		return &cacheResult{}, errors.Wrapf(dferrors.ErrResourceExpired, "url:%s, expireInfo:%v", task.Url, fileMetaData.ExpireInfo)
	}
	// not expired
	if fileMetaData.Finish {
		// check data integrity quickly by meta data
		return cd.parseByReadMetaFile(ctx, task.TaskID, fileMetaData)
	}
	// check source whether support range. if support, check cache by reading file data
	supportRange, err := cd.resourceClient.IsSupportRange(task.Url, task.Headers)
	if err != nil {
		return &cacheResult{}, errors.Wrapf(err, "check whether url(%s) support range fail", task.Url)
	}
	if !supportRange {
		logger.Errorf("taskID: %s taskUrl:%s not supports partial requests: %v", task.TaskID, task.Url, err)
		return &cacheResult{}, errors.Wrapf(dferrors.ErrResourceNotSupportRangeRequest, "url:%s", task.Url)

	}
	return cd.parseByReadFile(ctx, task.TaskID, fileMetaData)
}

// parseByReadMetaFile detect cache by metaData of file and pieces
func (cd *cacheDetector) parseByReadMetaFile(ctx context.Context, taskID string, fileMetaData *fileMetaData) (*cacheResult, error) {
	if !fileMetaData.Success {
		return &cacheResult{}, errors.Wrapf(dferrors.ErrDownloadFail, "success property in file meta data is false")
	}
	pieceMetaRecords, err := cd.metaDataManager.readAndCheckPieceMetaRecords(ctx, taskID, fileMetaData.SourceMd5)
	if err != nil {
		return &cacheResult{}, errors.Wrapf(err, "failed to read piece meta data from disk")
	}
	if len(pieceMetaRecords) != fileMetaData.TotalPieceCount {
		return &cacheResult{}, errors.Wrapf(dferrors.ErrPieceCountNotEqual, "memory piece count(%d), disk piece count(%d)", len(pieceMetaRecords), fileMetaData.TotalPieceCount)
	}
	// check downloaded data integrity
	storageInfo, err := cd.cacheStore.Stat(ctx, getDownloadRawFunc(taskID))
	if err != nil {
		return &cacheResult{}, errors.Wrapf(err, "get cdn file length fail")
	}
	// check downloaded data integrity through file size
	if fileMetaData.CdnFileLength != storageInfo.Size {
		return &cacheResult{}, errors.Wrapf(dferrors.ErrFileLengthNotEqual, "meta size %d, disk size %d", fileMetaData.CdnFileLength, storageInfo.Size)
	}
	return &cacheResult{
		breakNum:             -1,
		downloadedFileLength: fileMetaData.CdnFileLength,
		pieceMetaRecords:     pieceMetaRecords,
		fileMetaData:         fileMetaData,
		fileMd5:              nil,
	}, nil
}

// parseByReadFile
func (cd *cacheDetector) parseByReadFile(ctx context.Context, taskID string, metaData *fileMetaData) (*cacheResult, error) {
	reader, err := cd.cacheStore.Get(ctx, getDownloadRawFunc(taskID))
	if err != nil {
		return &cacheResult{}, errors.Wrapf(err, "failed to read key file")
	}
	// todo remove memory piece data
	// read piece records
	pieceMetaRecords, err := cd.metaDataManager.readWithoutCheckPieceMetaRecords(ctx, taskID)
	if err != nil {
		return &cacheResult{}, errors.Wrapf(err, "failed to read piece meta file")
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
	return &cacheResult{
		breakNum:             breakIndex,
		downloadedFileLength: downloadedFileLength,
		pieceMetaRecords:     pieceMetaRecords[0:breakIndex],
		fileMetaData:         metaData,
		fileMd5:              fileMd5,
	}, nil
}

// resetRepo
func (cd *cacheDetector) resetRepo(ctx context.Context, task *types.SeedTask) (*fileMetaData, error) {
	logger.Infof("taskID: %s reset repo for task", task.TaskID)
	if err := deleteTaskFiles(ctx, cd.cacheStore, task.TaskID); err != nil {
		logger.Errorf("taskID: %s reset repo: failed to delete task files: %v", task.TaskID, err)
	}
	// initialize meta data file
	return cd.metaDataManager.writeFileMetaDataByTask(ctx, task)
}

// checkSameFile check whether meta file is modified
func checkSameFile(task *types.SeedTask, metaData *fileMetaData) error {
	if task == nil || metaData == nil {
		return errors.Errorf("task or metaData is nil, task:%v, metaData:%v", task, metaData)
	}

	if metaData.PieceSize != task.PieceSize {
		return errors.Errorf("meta piece size(%d) is not equals with task piece size(%d)", metaData.PieceSize, task.PieceSize)
	}

	if metaData.TaskID != task.TaskID {
		return errors.Errorf("meta task taskID(%s) is not equals with task taskID(%s)", metaData.TaskID, task.TaskID)
	}

	if metaData.URL != task.Url {
		return errors.Errorf("meta task url(%s) is not equals with task url(%s)", metaData.URL, task.Url)
	}
	if !stringutils.IsEmptyStr(metaData.SourceMd5) && !stringutils.IsEmptyStr(task.RequestMd5) && metaData.SourceMd5 != task.RequestMd5 {
		return errors.Errorf("meta task source md5(%s) is not equals with task request md5(%s)", metaData.SourceMd5, task.RequestMd5)
	}
	return nil
}
