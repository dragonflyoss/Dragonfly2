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
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"d7y.io/dragonfly/v2/cdnsystem/daemon/mgr/cdn/storage"
	"d7y.io/dragonfly/v2/cdnsystem/storedriver"
	"d7y.io/dragonfly/v2/cdnsystem/types"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/synclock"
	"d7y.io/dragonfly/v2/pkg/util/digestutils"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
	"github.com/pkg/errors"
)

// cacheDataManager manages the meta file and piece meta file of each TaskId.
type cacheDataManager struct {
	storage     storage.Manager
	cacheLocker *synclock.LockerPool
}

func newCacheDataManager(storeMgr storage.Manager) *cacheDataManager {
	return &cacheDataManager{
		storeMgr,
		synclock.NewLockerPool(),
	}
}

// writeFileMetaDataByTask stores the metadata of task by task to storage.
func (mm *cacheDataManager) writeFileMetaDataByTask(ctx context.Context, task *types.SeedTask) (*storage.FileMetaData, error) {
	mm.cacheLocker.Lock(task.TaskID, false)
	defer mm.cacheLocker.UnLock(task.TaskID, false)
	metaData := &storage.FileMetaData{
		TaskId:          task.TaskID,
		TaskURL:         task.TaskURL,
		PieceSize:       task.PieceSize,
		SourceFileLen:   task.SourceFileLength,
		AccessTime:      getCurrentTimeMillisFunc(),
		CdnFileLength:   task.CdnFileLength,
		TotalPieceCount: task.PieceTotal,
	}

	if err := mm.storage.WriteFileMetaData(ctx, task.TaskID, metaData); err != nil {
		return nil, errors.Wrapf(err, "failed to write file metadata to storage")
	}

	return metaData, nil
}

// updateAccessTime update access and interval
func (mm *cacheDataManager) updateAccessTime(ctx context.Context, taskID string, accessTime int64) error {
	mm.cacheLocker.Lock(taskID, false)
	defer mm.cacheLocker.UnLock(taskID, false)

	originMetaData, err := mm.readFileMetaData(ctx, taskID)
	if err != nil {
		return err
	}
	// access interval
	interval := accessTime - originMetaData.AccessTime
	originMetaData.Interval = interval
	if interval <= 0 {
		logger.WithTaskID(taskID).Warnf("file hit interval:%d, accessTime:%v", interval, time.Unix(accessTime/1000, accessTime%1000))
		originMetaData.Interval = 0
	}

	originMetaData.AccessTime = accessTime

	return mm.storage.WriteFileMetaData(ctx, taskID, originMetaData)
}

func (mm *cacheDataManager) updateExpireInfo(ctx context.Context, taskID string, expireInfo map[string]string) error {
	mm.cacheLocker.Lock(taskID, false)
	defer mm.cacheLocker.UnLock(taskID, false)

	originMetaData, err := mm.readFileMetaData(ctx, taskID)
	if err != nil {
		return err
	}

	originMetaData.ExpireInfo = expireInfo

	return mm.storage.WriteFileMetaData(ctx, taskID, originMetaData)
}

func (mm *cacheDataManager) updateStatusAndResult(ctx context.Context, taskID string, metaData *storage.FileMetaData) error {
	mm.cacheLocker.Lock(taskID, false)
	defer mm.cacheLocker.UnLock(taskID, false)

	originMetaData, err := mm.readFileMetaData(ctx, taskID)
	if err != nil {
		return err
	}

	originMetaData.Finish = metaData.Finish
	originMetaData.Success = metaData.Success
	if originMetaData.Success {
		originMetaData.CdnFileLength = metaData.CdnFileLength
		originMetaData.SourceFileLen = metaData.SourceFileLen
		if metaData.TotalPieceCount > 0 {
			originMetaData.TotalPieceCount = metaData.TotalPieceCount
		}
		if !stringutils.IsBlank(metaData.SourceRealMd5) {
			originMetaData.SourceRealMd5 = metaData.SourceRealMd5
		}
		if !stringutils.IsBlank(metaData.PieceMd5Sign) {
			originMetaData.PieceMd5Sign = metaData.PieceMd5Sign
		}
	}
	return mm.storage.WriteFileMetaData(ctx, taskID, originMetaData)
}

// appendPieceMetaData append piece meta info to storage
func (mm *cacheDataManager) appendPieceMetaData(ctx context.Context, taskID string, record *storage.PieceMetaRecord) error {
	mm.cacheLocker.Lock(taskID, false)
	defer mm.cacheLocker.UnLock(taskID, false)
	// write to the storage
	return mm.storage.AppendPieceMetaData(ctx, taskID, record)
}

// appendPieceMetaData append piece meta info to storage
func (mm *cacheDataManager) writePieceMetaRecords(ctx context.Context, taskID string, records []*storage.PieceMetaRecord) error {
	mm.cacheLocker.Lock(taskID, false)
	defer mm.cacheLocker.UnLock(taskID, false)
	// write to the storage
	return mm.storage.WritePieceMetaRecords(ctx, taskID, records)
}

// readAndCheckPieceMetaRecords reads pieceMetaRecords from storage and check data integrity by the md5 file of the TaskId
func (mm *cacheDataManager) readAndCheckPieceMetaRecords(ctx context.Context, taskID, pieceMd5Sign string) ([]*storage.PieceMetaRecord, error) {
	mm.cacheLocker.Lock(taskID, true)
	defer mm.cacheLocker.UnLock(taskID, true)
	md5Sign, pieceMetaRecords, err := mm.getPieceMd5Sign(ctx, taskID)
	if err != nil {
		return nil, err
	}
	if md5Sign != pieceMd5Sign {
		return nil, fmt.Errorf("check piece meta data integrity fail, expectMd5Sign:%s, actualMd5Sign:%s",
			pieceMd5Sign, md5Sign)
	}
	return pieceMetaRecords, nil
}

// readPieceMetaRecords reads pieceMetaRecords from storage and without check data integrity
func (mm *cacheDataManager) readPieceMetaRecords(ctx context.Context, taskID string) ([]*storage.PieceMetaRecord, error) {
	mm.cacheLocker.Lock(taskID, true)
	defer mm.cacheLocker.UnLock(taskID, true)
	return mm.storage.ReadPieceMetaRecords(ctx, taskID)
}

func (mm *cacheDataManager) getPieceMd5Sign(ctx context.Context, taskID string) (string, []*storage.PieceMetaRecord, error) {
	pieceMetaRecords, err := mm.storage.ReadPieceMetaRecords(ctx, taskID)
	if err != nil {
		return "", nil, errors.Wrapf(err, "failed to read piece meta file")
	}
	var pieceMd5 []string
	for _, piece := range pieceMetaRecords {
		pieceMd5 = append(pieceMd5, piece.Md5)
	}
	return digestutils.Sha256(pieceMd5...), pieceMetaRecords, nil
}

func (mm *cacheDataManager) readFileMetaData(ctx context.Context, taskID string) (*storage.FileMetaData, error) {
	fileMeta, err := mm.storage.ReadFileMetaData(ctx, taskID)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read file metadata from storage")
	}
	return fileMeta, nil
}

func (mm *cacheDataManager) statDownloadFile(ctx context.Context, taskID string) (*storedriver.StorageInfo, error) {
	return mm.storage.StatDownloadFile(ctx, taskID)
}

func (mm *cacheDataManager) readDownloadFile(ctx context.Context, taskID string) (io.ReadCloser, error) {
	return mm.storage.ReadDownloadFile(ctx, taskID)
}

func (mm *cacheDataManager) resetRepo(ctx context.Context, task *types.SeedTask) error {
	mm.cacheLocker.Lock(task.TaskID, false)
	defer mm.cacheLocker.UnLock(task.TaskID, false)
	return mm.storage.ResetRepo(ctx, task)
}

func (mm *cacheDataManager) writeDownloadFile(ctx context.Context, taskID string, offset int64, len int64, buf *bytes.Buffer) error {
	return mm.storage.WriteDownloadFile(ctx, taskID, offset, len, buf)
}
