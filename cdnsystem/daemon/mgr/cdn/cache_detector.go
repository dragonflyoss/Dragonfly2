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
	"d7y.io/dragonfly/v2/cdnsystem/cdnerrors"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/mgr/cdn/storage"
	"d7y.io/dragonfly/v2/cdnsystem/source"
	"d7y.io/dragonfly/v2/cdnsystem/types"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
	"github.com/pkg/errors"
	"hash"
	"sort"
)

// cacheDetector detect task cache
type cacheDetector struct {
	cacheStore      storage.Storage
	metaDataManager *metaDataManager
	resourceClient  source.ResourceClient
}

// cacheResult cache result of detect
type cacheResult struct {
	breakNum         int32              // break-point of task file
	pieceMetaRecords []*pieceMetaRecord // piece meta data records of task
	fileMetaData     *fileMetaData      // file meta data of task
	fileMd5          hash.Hash          // md5 of file content that has been downloaded
}

// newCacheDetector create a new cache detector
func newCacheDetector(cacheStore storage.Storage, metaDataManager *metaDataManager,
	resourceClient source.ResourceClient) *cacheDetector {
	return &cacheDetector{
		cacheStore:      cacheStore,
		metaDataManager: metaDataManager,
		resourceClient:  resourceClient,
	}
}

// detectCache detect cache which has been downloaded and stored on cacheStore
func (cd *cacheDetector) detectCache(ctx context.Context, task *types.SeedTask) (*cacheResult, error) {
	detectResult, err := cd.doDetect(ctx, task)
	logger.WithTaskID(task.TaskId).Debugf("detects cache result:%v", detectResult)
	if err != nil {
		logger.WithTaskID(task.TaskId).Warnf("detect cache encounter an error: %v", err)
		metaData, err := cd.resetRepo(ctx, task)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to reset repo")
		}
		detectResult = &cacheResult{
			breakNum:         0,
			pieceMetaRecords: nil,
			fileMetaData:     metaData,
			fileMd5:          nil,
		}
	} else {
		logger.WithTaskID(task.TaskId).Debugf("start to update access time")
		if err := cd.metaDataManager.updateAccessTime(ctx, task.TaskId, getCurrentTimeMillisFunc()); err != nil {
			logger.WithTaskID(task.TaskId).Warnf("failed to update task access time ")
		}
	}
	return detectResult, nil
}

// doDetect the actual detect action which detects file metaData and pieces metaData of specific task
func (cd *cacheDetector) doDetect(ctx context.Context, task *types.SeedTask) (*cacheResult, error) {
	logger.Debugf("create upload symbol link")
	fileMetaData, err := cd.metaDataManager.readFileMetaData(ctx, task.TaskId)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read file meta data from store")
	}
	if err := checkSameFile(task, fileMetaData); err != nil {
		return nil, errors.Wrapf(err, "task does not match meta information of task file ")
	}
	expired, err := cd.resourceClient.IsExpired(task.Url, task.Headers, fileMetaData.ExpireInfo)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to check if the task expired")
	}
	logger.WithTaskID(task.TaskId).Debugf("success to get expired result: %t", expired)
	if expired {
		return nil, errors.Wrapf(cdnerrors.ErrResourceExpired, "url:%s, expireInfo:%+v", task.Url,
			fileMetaData.ExpireInfo)
	}
	// not expired
	if fileMetaData.Finish {
		// quickly detect the cache situation through the meta data
		return cd.parseByReadMetaFile(ctx, task.TaskId, fileMetaData)
	}
	// check if the resource supports range request. if so,
	// detect the cache situation by reading piece meta and data file
	supportRange, err := cd.resourceClient.IsSupportRange(task.Url, task.Headers)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to check if url(%s) supports range request", task.Url)
	}
	if !supportRange {
		return nil, errors.Wrapf(cdnerrors.ErrResourceNotSupportRangeRequest, "url:%s", task.Url)
	}
	return cd.parseByReadFile(ctx, task.TaskId, fileMetaData)
}

// parseByReadMetaFile detect cache by read meta and pieceMeta files of task
func (cd *cacheDetector) parseByReadMetaFile(ctx context.Context, taskId string,
	fileMetaData *fileMetaData) (*cacheResult, error) {
	if !fileMetaData.Success {
		return nil, errors.Wrapf(cdnerrors.ErrDownloadFail, "success flag of meta is false")
	}
	pieceMetaRecords, err := cd.metaDataManager.readAndCheckPieceMetaRecords(ctx, taskId, fileMetaData.SourceRealMd5)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read piece meta data from storage")
	}
	if fileMetaData.TotalPieceCount > 0 && len(pieceMetaRecords) != int(fileMetaData.TotalPieceCount) {
		return nil, errors.Wrapf(cdnerrors.ErrPieceCountNotEqual, "piece meta file piece count(%d), " +
			"meta file piece count(%d)", len(pieceMetaRecords), fileMetaData.TotalPieceCount)
	}
	storageInfo, err := cd.cacheStore.StatDownloadFile(ctx, taskId)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get cdn cache file length")
	}
	// check file data integrity by file size
	if fileMetaData.CdnFileLength != storageInfo.Size {
		return nil, errors.Wrapf(cdnerrors.ErrFileLengthNotEqual, "meta size %d, disk size %d",
			fileMetaData.CdnFileLength, storageInfo.Size)
	}
	return &cacheResult{
		breakNum:         -1,
		pieceMetaRecords: pieceMetaRecords,
		fileMetaData:     fileMetaData,
		fileMd5:          nil,
	}, nil
}

// parseByReadFile detect cache by read pieceMeta and data files of task
func (cd *cacheDetector) parseByReadFile(ctx context.Context, taskId string, metaData *fileMetaData) (*cacheResult, error) {
	reader, err := cd.cacheStore.ReadDownloadFile(ctx, taskId)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read file")
	}
	pieceMetaRecords, err := cd.metaDataManager.readPieceMetaRecordsWithoutCheck(ctx, taskId)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read piece meta file")
	}

	fileMd5 := md5.New()
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
			logger.WithTaskID(taskId).Errorf("read content of pieceNum %d enter err: %v", record.PieceNum, err)
			breakIndex = index
			break
		}
		breakIndex = index + 1
	}
	// todo 清除不连续的分片元数据信息
	return &cacheResult{
		breakNum:         int32(breakIndex),
		pieceMetaRecords: pieceMetaRecords[0:breakIndex:breakIndex],
		fileMetaData:     metaData,
		fileMd5:          fileMd5,
	}, nil
}

// resetRepo
func (cd *cacheDetector) resetRepo(ctx context.Context, task *types.SeedTask) (*fileMetaData, error) {
	logger.WithTaskID(task.TaskId).Info("reset repo for task")
	err := cd.cacheStore.ResetRepo(ctx, task)
	if err != nil {
		return nil, err
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
		return errors.Errorf("meta piece size(%d) is not equals with task piece size(%d)", metaData.PieceSize,
			task.PieceSize)
	}

	if metaData.TaskId != task.TaskId {
		return errors.Errorf("meta task TaskId(%s) is not equals with task TaskId(%s)", metaData.TaskId, task.TaskId)
	}

	if metaData.TaskURL != task.TaskUrl {
		return errors.Errorf("meta task taskUrl(%s) is not equals with task taskUrl(%s)", metaData.TaskURL, task.Url)
	}
	if !stringutils.IsBlank(metaData.SourceRealMd5) && !stringutils.IsBlank(task.RequestMd5) &&
		metaData.SourceRealMd5 != task.RequestMd5 {
		return errors.Errorf("meta task source md5(%s) is not equals with task request md5(%s)",
			metaData.SourceRealMd5, task.RequestMd5)
	}
	return nil
}
