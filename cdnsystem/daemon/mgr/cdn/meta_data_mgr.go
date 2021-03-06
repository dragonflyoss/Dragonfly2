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
	"d7y.io/dragonfly/v2/pkg/dferrors"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/digest"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
	"github.com/pkg/errors"
)

// fileMetaData
type FileMetaData struct {
	TaskId          string            `json:"taskId"`
	TaskURL         string            `json:"taskUrl"`
	PieceSize       int32             `json:"pieceSize"`
	SourceFileLen   int64             `json:"sourceFileLen"`
	AccessTime      int64             `json:"accessTime"`
	Interval        int64             `json:"interval"`
	CdnFileLength   int64             `json:"cdnFileLength"`
	SourceRealMd5   string            `json:"sourceRealMd5"`
	PieceMd5Sign    string            `json:"pieceMd5Sign"`
	ExpireInfo      map[string]string `json:"expireInfo"`
	Finish          bool              `json:"finish"`
	Success         bool              `json:"success"`
	TotalPieceCount int32             `json:"totalPieceCount"`
}

// pieceMetaRecord
type PieceMetaRecord struct {
	PieceNum   int32             `json:"pieceNum"`   // piece Num start from 0
	PieceLen   int32             `json:"pieceLen"`   // 下载存储的真实长度
	Md5        string            `json:"md5"`        // piece content md5
	Range      string            `json:"range"`      // 下载存储到磁盘的range，不一定是origin source的range
	Offset     uint64            `json:"offset"`     // offset
	PieceStyle types.PieceFormat `json:"pieceStyle"` // 1: PlainUnspecified
}

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
func (mm *metaDataManager) writeFileMetaDataByTask(ctx context.Context, task *types.SeedTask) (*FileMetaData, error) {
	metaData := &FileMetaData{
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

func (mm *metaDataManager) updateStatusAndResult(ctx context.Context, taskId string, metaData *FileMetaData) error {
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
		if !stringutils.IsBlank(metaData.SourceRealMd5) {
			originMetaData.SourceRealMd5 = metaData.SourceRealMd5
		}
		if !stringutils.IsBlank(metaData.PieceMd5Sign) {
			originMetaData.PieceMd5Sign = metaData.PieceMd5Sign
		}
	}
	return mm.fileStore.WriteFileMetaData(ctx, taskId, originMetaData)
}

// appendPieceMetaDataToFile append piece meta info to storage
func (mm *metaDataManager) appendPieceMetaData(ctx context.Context, taskId string,
	record *PieceMetaRecord) error {
	mm.pieceMetaLocker.GetLock(taskId, false)
	defer mm.pieceMetaLocker.ReleaseLock(taskId, false)
	// write to the storage
	return mm.fileStore.AppendPieceMetaData(ctx, taskId, record)
}

func (mm *metaDataManager) appendPieceMetaIntegrityData(ctx context.Context, taskId, fileMD5 string) error {
	mm.pieceMetaLocker.GetLock(taskId, false)
	defer mm.pieceMetaLocker.ReleaseLock(taskId, false)
	return mm.fileStore.AppendPieceMetaIntegrityData(ctx, taskId, fileMD5)
}

// readAndCheckPieceMetaRecords reads pieceMetaRecords from storage and check data integrity by the md5 file of the TaskId
func (mm *metaDataManager) readPieceMetaRecords(ctx context.Context, taskId,
	fileMD5 string) ([]*PieceMetaRecord, error) {
	mm.pieceMetaLocker.GetLock(taskId, true)
	defer mm.pieceMetaLocker.ReleaseLock(taskId, true)
	return mm.fileStore.ReadPieceMetaRecords(ctx, taskId, fileMD5)
}

// readPieceMetaRecordsWithoutCheck reads pieceMetaRecords from storage and without check data integrity
func (mm *metaDataManager) readPieceMetaRecordsWithoutCheck(ctx context.Context, taskId string) ([]*PieceMetaRecord,
	error) {
	pieceMetaRecords, err := mm.fileStore.ReadPieceMetaRecords(ctx, taskId, "")
	if err != nil {
		return nil, err
	}

	if len(pieceMetaRecords) == 0 {
		return nil, dferrors.ErrDataNotFound
	}
	return pieceMetaRecords, nil
}

func (mm *metaDataManager) getPieceMd5Sign(ctx context.Context, taskId string) (md5Sign string, err error) {
	pieceMetaRecords, err := mm.readPieceMetaRecordsWithoutCheck(ctx, taskId)
	if err != nil {
		return "", errors.Wrapf(err, "failed to read piece meta file")
	}
	var pieceMd5 []string
	for _, piece := range pieceMetaRecords {
		pieceMd5 = append(pieceMd5, piece.Md5)
	}
	return digest.Sha1(pieceMd5), nil
}

func (mm *metaDataManager) readFileMetaData(ctx context.Context, taskId string) (*FileMetaData, error) {
	if fileMeta, err := mm.fileStore.ReadFileMetaData(ctx, taskId); err != nil {
		return nil, errors.Wrapf(err, "failed to read file metadata from storage")
	} else {
		return fileMeta, nil
	}
}
