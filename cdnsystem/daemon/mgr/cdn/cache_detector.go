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
	"fmt"
	"github.com/pkg/errors"
	"hash"
	"sort"
)

// cacheDetector detect task cache
type cacheDetector struct {
	cacheDataManager *cacheDataManager
	resourceClient   source.ResourceClient
}

// cacheResult cache result of detect
type cacheResult struct {
	breakPoint       int64                      // break-point of task file
	pieceMetaRecords []*storage.PieceMetaRecord // piece meta data records of task
	fileMetaData     *storage.FileMetaData      // file meta data of task
	fileMd5          hash.Hash                  // md5 of file content that has been downloaded
}

func (s *cacheResult) String() string {
	return fmt.Sprintf("{breakNum:%d, pieceMetaRecords:%+v, fileMetaData:%+v, "+
		"fileMd5:%v}", s.breakPoint, s.pieceMetaRecords, s.fileMetaData, s.fileMd5)
}

// newCacheDetector create a new cache detector
func newCacheDetector(cacheDataManager *cacheDataManager, resourceClient source.ResourceClient) *cacheDetector {
	return &cacheDetector{
		cacheDataManager: cacheDataManager,
		resourceClient:   resourceClient,
	}
}

func (cd *cacheDetector) detectCache(ctx context.Context, task *types.SeedTask) (*cacheResult, error) {
	//err := cd.cacheStore.CreateUploadLink(ctx, task.TaskId)
	//if err != nil {
	//	return nil, errors.Wrapf(err, "failed to create upload symbolic link")
	//}
	result, err := cd.doDetect(ctx, task)
	if err != nil {
		logger.WithTaskID(task.TaskId).Infof("failed to detect cache, reset cache: %v", err)
		metaData, err := cd.resetCache(ctx, task)
		if err == nil {
			result = &cacheResult{
				fileMetaData: metaData,
			}
			return result, nil
		}
		return result, err
	} else {
		if err := cd.cacheDataManager.updateAccessTime(ctx, task.TaskId, getCurrentTimeMillisFunc()); err != nil {
			logger.WithTaskID(task.TaskId).Warnf("failed to update task access time ")
		}
	}
	return result, nil
}

// detectCache the actual detect action which detects file metaData and pieces metaData of specific task
func (cd *cacheDetector) doDetect(ctx context.Context, task *types.SeedTask) (result *cacheResult, err error) {
	fileMetaData, err := cd.cacheDataManager.readFileMetaData(ctx, task.TaskId)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read file meta data")
	}
	if err := checkSameFile(task, fileMetaData); err != nil {
		return nil, errors.Wrapf(err, "task does not match meta information of task file")
	}
	expired, err := cd.resourceClient.IsExpired(task.Url, task.Header, fileMetaData.ExpireInfo)
	if err != nil {
		// 如果获取失败，则认为没有过期，防止打爆源
		logger.WithTaskID(task.TaskId).Errorf("failed to check if the task expired: %v", err)
	}
	logger.WithTaskID(task.TaskId).Debugf("task expired result: %t", expired)
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
	supportRange, err := cd.resourceClient.IsSupportRange(task.Url, task.Header)
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
	fileMetaData *storage.FileMetaData) (*cacheResult, error) {
	if !fileMetaData.Success {
		return nil, errors.Wrapf(cdnerrors.ErrDownloadFail, "success flag of download is false")
	}
	pieceMetaRecords, err := cd.cacheDataManager.readAndCheckPieceMetaRecords(ctx, taskId, fileMetaData.PieceMd5Sign)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to check piece meta integrity")
	}
	if fileMetaData.TotalPieceCount > 0 && len(pieceMetaRecords) != int(fileMetaData.TotalPieceCount) {
		return nil, errors.Wrapf(cdnerrors.ErrPieceCountNotEqual, "piece file piece count(%d), "+
			"meta file piece count(%d)", len(pieceMetaRecords), fileMetaData.TotalPieceCount)
	}
	storageInfo, err := cd.cacheDataManager.statDownloadFile(ctx, taskId)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get cdn file length")
	}
	// check file data integrity by file size
	if fileMetaData.CdnFileLength != storageInfo.Size {
		return nil, errors.Wrapf(cdnerrors.ErrFileLengthNotEqual, "meta size %d, storage size %d",
			fileMetaData.CdnFileLength, storageInfo.Size)
	}
	return &cacheResult{
		breakPoint:       -1,
		pieceMetaRecords: pieceMetaRecords,
		fileMetaData:     fileMetaData,
		fileMd5:          nil,
	}, nil
}

// parseByReadFile detect cache by read pieceMeta and data files of task
func (cd *cacheDetector) parseByReadFile(ctx context.Context, taskId string, metaData *storage.FileMetaData) (*cacheResult, error) {
	reader, err := cd.cacheDataManager.readDownloadFile(ctx, taskId)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read data file")
	}
	defer reader.Close()
	tempRecords, err := cd.cacheDataManager.readPieceMetaRecords(ctx, taskId)
	if err != nil {
		return nil, errors.Wrapf(err, "parseByReadFile:failed to read piece meta file")
	}

	fileMd5 := md5.New()
	// sort piece meta records by pieceNum
	sort.Slice(tempRecords, func(i, j int) bool {
		return tempRecords[i].PieceNum < tempRecords[j].PieceNum
	})

	var breakPoint uint64 = 0
	pieceMetaRecords := make([]*storage.PieceMetaRecord, 0, 0)
	for index := range tempRecords {
		if int32(index) != tempRecords[index].PieceNum {
			break
		}
		// read content
		if err := checkPieceContent(reader, tempRecords[index], fileMd5); err != nil {
			logger.WithTaskID(taskId).Errorf("read content of pieceNum %d failed: %v", tempRecords[index].PieceNum, err)
			break
		}
		breakPoint = tempRecords[index].OriginRange.EndIndex + 1
		pieceMetaRecords = append(pieceMetaRecords, tempRecords[index])
	}
	if len(tempRecords) != len(pieceMetaRecords) {
		if err := cd.cacheDataManager.writePieceMetaRecords(ctx, taskId, pieceMetaRecords); err != nil {
			return nil, errors.Wrapf(err, "write piece meta records failed")
		}
	}
	// todo already download done, piece 信息已经写完但是meta信息还没有完成更新
	//if metaData.SourceFileLen >=0 && int64(breakPoint) == metaData.SourceFileLen {
	//	return &cacheResult{
	//		breakPoint:       -1,
	//		pieceMetaRecords: pieceMetaRecords,
	//		fileMetaData:     metaData,
	//		fileMd5:          fileMd5,
	//	}, nil
	//}
	// todo 整理数据文件 truncate breakpoint之后的数据内容
	return &cacheResult{
		breakPoint:       int64(breakPoint),
		pieceMetaRecords: pieceMetaRecords,
		fileMetaData:     metaData,
		fileMd5:          fileMd5,
	}, nil
}

// resetCache
func (cd *cacheDetector) resetCache(ctx context.Context, task *types.SeedTask) (*storage.FileMetaData, error) {
	err := cd.cacheDataManager.resetRepo(ctx, task)
	if err != nil {
		return nil, err
	}
	// initialize meta data file
	return cd.cacheDataManager.writeFileMetaDataByTask(ctx, task)
}
