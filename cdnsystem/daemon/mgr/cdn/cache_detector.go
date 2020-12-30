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
	breakNum             int32             // break-point
	downloadedFileLength int64             // length of file has been downloaded
	pieceMetaRecords     []pieceMetaRecord // piece meta data records of task
	fileMetaData         *fileMetaData     // file meta data of task
	fileMd5              hash.Hash         // md5 of file content that has been downloaded
}

// newCacheDetector create a new cache detector
func newCacheDetector(cacheStore *store.Store, metaDataManager *metaDataManager, resourceClient source.ResourceClient) *cacheDetector {
	return &cacheDetector{
		cacheStore:      cacheStore,
		metaDataManager: metaDataManager,
		resourceClient:  resourceClient,
	}
}

// detectCache detect cache which has been stored on cacheStore
func (cd *cacheDetector) detectCache(ctx context.Context, task *types.SeedTask) (*cacheResult, error) {
	detectResult, err := cd.doDetect(ctx, task)
	logger.Named(task.TaskID).Debugf("detects cache result:%v", detectResult)
	if err != nil {
		logger.Named(task.TaskID).Errorf("failed to detect cache:%v", err)
		metaData, err := cd.resetRepo(ctx, task)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to reset repo")
		}
		detectResult = &cacheResult{
			breakNum:             0,
			downloadedFileLength: 0,
			pieceMetaRecords:     nil,
			fileMetaData:         metaData,
			fileMd5:              nil,
		}
	} else {
		logger.Named(task.TaskID).Debugf("start to update access time")
		if err := cd.metaDataManager.updateAccessTime(ctx, task.TaskID, getCurrentTimeMillisFunc()); err != nil {
			logger.Named(task.TaskID).Warnf("failed to update task access time ")
		}
	}
	return detectResult, nil
}

// doDetect do detect action which detects file metaData and pieces metaData of specific task
func (cd *cacheDetector) doDetect(ctx context.Context, task *types.SeedTask) (*cacheResult, error) {
	fileMetaData, err := cd.metaDataManager.readFileMetaData(ctx, task.TaskID)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read file meta data from store")
	}
	if err := checkSameFile(task, fileMetaData); err != nil {
		return nil, errors.Wrapf(err, "failed to check same file")
	}
	expired, err := cd.resourceClient.IsExpired(task.Url, task.Headers, fileMetaData.ExpireInfo)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to check if the task expired")
	}
	logger.Named(task.TaskID).Debugf("success to get expired result: %t", expired)
	if expired {
		return nil, errors.Wrapf(dferrors.ErrResourceExpired, "url:%s, expireInfo:%+v", task.Url, fileMetaData.ExpireInfo)
	}
	// not expired
	if fileMetaData.Finish {
		// quickly detect the cache situation through the meta data
		return cd.parseByReadMetaFile(ctx, task.TaskID, fileMetaData)
	}
	// check if the resource supports range request. if so, detect the cache situation by reading piece meta and data file
	supportRange, err := cd.resourceClient.IsSupportRange(task.Url, task.Headers)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to check if url(%s) support range request", task.Url)
	}
	if !supportRange {
		return nil, errors.Wrapf(dferrors.ErrResourceNotSupportRangeRequest, "url:%s", task.Url)
	}
	return cd.parseByReadFile(ctx, task.TaskID, fileMetaData)
}

// parseByReadMetaFile detect cache by read meta and pieceMeta files of task
func (cd *cacheDetector) parseByReadMetaFile(ctx context.Context, taskID string, fileMetaData *fileMetaData) (*cacheResult, error) {
	if !fileMetaData.Success {
		return nil, errors.Wrapf(dferrors.ErrDownloadFail, "success property flag is false")
	}
	pieceMetaRecords, err := cd.metaDataManager.readAndCheckPieceMetaRecords(ctx, taskID, fileMetaData.SourceMd5)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read piece meta data from storage")
	}
	if fileMetaData.TotalPieceCount > 0 && len(pieceMetaRecords) != int(fileMetaData.TotalPieceCount) {
		return nil, errors.Wrapf(dferrors.ErrPieceCountNotEqual, "piece meta file piece count(%d), meta file piece count(%d)", len(pieceMetaRecords), fileMetaData.TotalPieceCount)
	}
	storageInfo, err := cd.cacheStore.Stat(ctx, getDownloadRawFunc(taskID))
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get cdn file length")
	}
	// check file data integrity by file size
	if fileMetaData.CdnFileLength != storageInfo.Size {
		return nil, errors.Wrapf(dferrors.ErrFileLengthNotEqual, "meta size %d, disk size %d", fileMetaData.CdnFileLength, storageInfo.Size)
	}
	return &cacheResult{
		breakNum:             -1,
		downloadedFileLength: fileMetaData.CdnFileLength,
		pieceMetaRecords:     pieceMetaRecords,
		fileMetaData:         fileMetaData,
		fileMd5:              nil,
	}, nil
}

// parseByReadFile detect cache by read pieceMeta and data files of task
func (cd *cacheDetector) parseByReadFile(ctx context.Context, taskID string, metaData *fileMetaData) (*cacheResult, error) {
	reader, err := cd.cacheStore.Get(ctx, getDownloadRawFunc(taskID))
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read file")
	}
	pieceMetaRecords, err := cd.metaDataManager.readWithoutCheckPieceMetaRecords(ctx, taskID)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read piece meta file")
	}

	fileMd5 := md5.New()
	var downloadedFileLength int64 = 0
	// sort piece meta records by pieceNum
	sort.Slice(pieceMetaRecords, func(i, j int) bool {
		return pieceMetaRecords[i].PieceNum < pieceMetaRecords[j].PieceNum
	})

	var breakIndex int
	for index, record := range pieceMetaRecords {
		if int32(index) != record.PieceNum {
			breakIndex = index
			break
		}
		// read content
		if err := checkPieceContent(reader, record, fileMd5); err != nil {
			logger.Named(taskID).Errorf("failed to read content of pieceNum %d: %v", record.PieceNum, err)
			breakIndex = index
			break
		}
		downloadedFileLength += int64(record.PieceLen)
	}
	// todo 清除不连续的分片元数据信息
	return &cacheResult{
		breakNum:             int32(breakIndex),
		downloadedFileLength: downloadedFileLength,
		pieceMetaRecords:     pieceMetaRecords[0:breakIndex],
		fileMetaData:         metaData,
		fileMd5:              fileMd5,
	}, nil
}

// resetRepo
func (cd *cacheDetector) resetRepo(ctx context.Context, task *types.SeedTask) (*fileMetaData, error) {
	logger.Named(task.TaskID).Info("reset repo for task")
	if err := deleteTaskFiles(ctx, cd.cacheStore, task.TaskID); err != nil {
		logger.Named(task.TaskID).Errorf("reset repo: failed to delete task files: %v", err)
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

	if metaData.TaskURL != task.TaskUrl {
		return errors.Errorf("meta task taskUrl(%s) is not equals with task taskUrl(%s)", metaData.TaskURL, task.Url)
	}
	if !stringutils.IsEmptyStr(metaData.SourceMd5) && !stringutils.IsEmptyStr(task.RequestMd5) && metaData.SourceMd5 != task.RequestMd5 {
		return errors.Errorf("meta task source md5(%s) is not equals with task request md5(%s)", metaData.SourceMd5, task.RequestMd5)
	}
	return nil
}
