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
	"d7y.io/dragonfly/v2/cdnsystem/daemon/mgr/cdn/storage"
	"d7y.io/dragonfly/v2/cdnsystem/types"
	"d7y.io/dragonfly/v2/cdnsystem/util"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/util/digestutils"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
	"fmt"
	"github.com/pkg/errors"
)

// fileMetaDataManager manages the meta file and piece meta file of each TaskId.
type metaDataManager struct {
	fileStore       storage.StorageMgr
	fileMetaLocker  *util.LockerPool
	pieceMetaLocker *util.LockerPool
}

func newFileMetaDataManager(storeMgr storage.StorageMgr) *metaDataManager {
	return &metaDataManager{
		fileStore:       storeMgr,
		fileMetaLocker:  util.NewLockerPool(),
		pieceMetaLocker: util.NewLockerPool(),
	}
}

// writeFileMetaDataByTask stores the metadata of task by task to storage.
func (mm *metaDataManager) writeFileMetaDataByTask(ctx context.Context, task *types.SeedTask) (*storage.FileMetaData,
	error) {
	metaData := &storage.FileMetaData{
		TaskId:          task.TaskId,
		TaskURL:         task.TaskUrl,
		PieceSize:       task.PieceSize,
		SourceFileLen:   task.SourceFileLength,
		AccessTime:      getCurrentTimeMillisFunc(),
		CdnFileLength:   task.CdnFileLength,
		TotalPieceCount: task.PieceTotal,
	}

	if err := mm.fileStore.WriteFileMetaData(ctx, task.TaskId, metaData); err != nil {
		return nil, errors.Wrapf(err, "failed to write file metadata to storage")
	}

	return metaData, nil
}

// updateAccessTime update access and interval
func (mm *metaDataManager) updateAccessTime(ctx context.Context, taskId string, accessTime int64) error {
	mm.fileMetaLocker.GetLock(taskId, false)
	defer mm.fileMetaLocker.ReleaseLock(taskId, false)

	originMetaData, err := mm.readFileMetaData(ctx, taskId)
	if err != nil {
		return err
	}
	// access interval
	interval := accessTime - originMetaData.AccessTime
	originMetaData.Interval = interval
	if interval <= 0 {
		logger.WithTaskID(taskId).Warnf("file hit interval:%d", interval)
		originMetaData.Interval = 0
	}

	originMetaData.AccessTime = accessTime

	return mm.fileStore.WriteFileMetaData(ctx, taskId, originMetaData)
}

func (mm *metaDataManager) updateExpireInfo(ctx context.Context, taskId string, expireInfo map[string]string) error {
	mm.fileMetaLocker.GetLock(taskId, false)
	defer mm.fileMetaLocker.ReleaseLock(taskId, false)

	originMetaData, err := mm.readFileMetaData(ctx, taskId)
	if err != nil {
		return err
	}

	originMetaData.ExpireInfo = expireInfo

	return mm.fileStore.WriteFileMetaData(ctx, taskId, originMetaData)
}

func (mm *metaDataManager) updateStatusAndResult(ctx context.Context, taskId string,
	metaData *storage.FileMetaData) error {
	mm.fileMetaLocker.GetLock(taskId, false)
	defer mm.fileMetaLocker.ReleaseLock(taskId, false)

	originMetaData, err := mm.readFileMetaData(ctx, taskId)
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
	return mm.fileStore.WriteFileMetaData(ctx, taskId, originMetaData)
}

// appendPieceMetaData append piece meta info to storage
func (mm *metaDataManager) appendPieceMetaData(ctx context.Context, taskId string,
	record *storage.PieceMetaRecord) error {
	mm.pieceMetaLocker.GetLock(taskId, false)
	defer mm.pieceMetaLocker.ReleaseLock(taskId, false)
	// write to the storage
	return mm.fileStore.AppendPieceMetaData(ctx, taskId, record)
}

// readAndCheckPieceMetaRecords reads pieceMetaRecords from storage and check data integrity by the md5 file of the TaskId
func (mm *metaDataManager) readAndCheckPieceMetaRecords(ctx context.Context, taskId,
	pieceMd5Sign string) ([]*storage.PieceMetaRecord, error) {
	mm.pieceMetaLocker.GetLock(taskId, true)
	defer mm.pieceMetaLocker.ReleaseLock(taskId, true)
	md5Sign, pieceMetaRecords , err := mm.getPieceMd5Sign(ctx, taskId)
	if err != nil {
		return nil, err
	}
	if md5Sign != pieceMd5Sign {
		return nil, fmt.Errorf("check piece meta data integrity fail, expectMd5Sign:%s, actualMd5Sign:%s", pieceMd5Sign,
			md5Sign)
	}
	return pieceMetaRecords, nil
}

// readPieceMetaRecordsWithoutCheck reads pieceMetaRecords from storage and without check data integrity
func (mm *metaDataManager) readPieceMetaRecordsWithoutCheck(ctx context.Context,
	taskId string) ([]*storage.PieceMetaRecord, error) {
	mm.pieceMetaLocker.GetLock(taskId, true)
	defer mm.pieceMetaLocker.ReleaseLock(taskId, true)
	return mm.fileStore.ReadPieceMetaRecords(ctx, taskId)
}

func (mm *metaDataManager) getPieceMd5Sign(ctx context.Context, taskId string) (string, []*storage.PieceMetaRecord, error) {
	pieceMetaRecords, err := mm.fileStore.ReadPieceMetaRecords(ctx, taskId)
	if err != nil {
		return "", nil, errors.Wrapf(err, "failed to read piece meta file")
	}
	var pieceMd5 []string
	for _, piece := range pieceMetaRecords {
		pieceMd5 = append(pieceMd5, piece.Md5)
	}
	return digestutils.Sha256(pieceMd5...), pieceMetaRecords,nil
}

func (mm *metaDataManager) readFileMetaData(ctx context.Context, taskId string) (*storage.FileMetaData, error) {
	if fileMeta, err := mm.fileStore.ReadFileMetaData(ctx, taskId); err != nil {
		return nil, errors.Wrapf(err, "failed to read file metadata from storage")
	} else {
		return fileMeta, nil
	}
}
