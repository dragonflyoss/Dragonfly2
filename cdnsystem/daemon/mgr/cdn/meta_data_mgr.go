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
	"encoding/json"
	"github.com/pkg/errors"
	"strings"
)

const FieldSeparator = ":"

// fileMetaData
type fileMetaData struct {
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
type pieceMetaRecord struct {
	PieceNum   int32             `json:"pieceNum"`   // piece Num start from 0
	PieceLen   int32             `json:"pieceLen"`   // 下载存储的真实长度
	Md5        string            `json:"md5"`        // piece content md5
	Range      string            `json:"range"`      // 下载存储到磁盘的range，不一定是origin source的range
	Offset     uint64            `json:"offset"`     // offset
	PieceStyle types.PieceFormat `json:"pieceStyle"` // 1: PlainUnspecified
}

// fileMetaDataManager manages the meta file and piece meta file of each TaskId.
type metaDataManager struct {
	fileStore       storage.Storage
	fileMetaLocker  *util.LockerPool
	pieceMetaLocker *util.LockerPool
}

func newFileMetaDataManager(store storage.Storage) *metaDataManager {
	return &metaDataManager{
		fileStore:       store,
		fileMetaLocker:  util.NewLockerPool(),
		pieceMetaLocker: util.NewLockerPool(),
	}
}

// writeFileMetaDataByTask stores the metadata of task by task to storage.
func (mm *metaDataManager) writeFileMetaDataByTask(ctx context.Context, task *types.SeedTask) (*fileMetaData, error) {
	metaData := &fileMetaData{
		TaskId:          task.TaskId,
		TaskURL:         task.TaskUrl,
		PieceSize:       task.PieceSize,
		SourceFileLen:   task.SourceFileLength,
		AccessTime:      getCurrentTimeMillisFunc(),
		CdnFileLength:   task.CdnFileLength,
		TotalPieceCount: task.PieceTotal,
	}

	if err := mm.writeFileMetaData(ctx, metaData); err != nil {
		return nil, errors.Wrapf(err, "failed to write file metadata")
	}

	return metaData, nil
}

// writeFileMetaData stores the metadata of TaskId to storage.
func (mm *metaDataManager) writeFileMetaData(ctx context.Context, metaData *fileMetaData) error {
	data, err := json.Marshal(metaData)
	if err != nil {
		return errors.Wrapf(err, "failed to marshal metadata")
	}
	return mm.fileStore.WriteFileMetaDataBytes(ctx, metaData.TaskId, data)
}

// readFileMetaData read task metadata information according to TaskId
func (mm *metaDataManager) readFileMetaData(ctx context.Context, taskId string) (*fileMetaData, error) {
	bytes, err := mm.fileStore.ReadFileMetaDataBytes(ctx, taskId)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get metadata bytes")
	}

	metaData := &fileMetaData{}
	if err := json.Unmarshal(bytes, metaData); err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal metadata bytes")
	}
	logger.WithTaskID(taskId).Debugf("success to read metadata: %+v ", metaData)
	return metaData, nil
}

// updateAccessTime update access and interval
func (mm *metaDataManager) updateAccessTime(ctx context.Context, taskId string, accessTime int64) error {
	mm.fileMetaLocker.GetLock(taskId, false)
	defer mm.fileMetaLocker.ReleaseLock(taskId, false)

	originMetaData, err := mm.readFileMetaData(ctx, taskId)
	if err != nil {
		return errors.Wrapf(err, "failed to get origin metaData")
	}
	// access interval
	interval := accessTime - originMetaData.AccessTime
	originMetaData.Interval = interval
	if interval <= 0 {
		logger.WithTaskID(taskId).Warnf("file hit interval:%d", interval)
		originMetaData.Interval = 0
	}

	originMetaData.AccessTime = accessTime

	return mm.writeFileMetaData(ctx, originMetaData)
}

func (mm *metaDataManager) updateExpireInfo(ctx context.Context, taskId string, expireInfo map[string]string) error {
	mm.fileMetaLocker.GetLock(taskId, false)
	defer mm.fileMetaLocker.ReleaseLock(taskId, false)

	originMetaData, err := mm.readFileMetaData(ctx, taskId)
	if err != nil {
		return err
	}

	originMetaData.ExpireInfo = expireInfo

	return mm.writeFileMetaData(ctx, originMetaData)
}

func (mm *metaDataManager) updateStatusAndResult(ctx context.Context, taskId string, metaData *fileMetaData) error {
	mm.fileMetaLocker.GetLock(taskId, false)
	defer mm.fileMetaLocker.ReleaseLock(taskId, false)

	originMetaData, err := mm.readFileMetaData(ctx, taskId)
	if err != nil {
		return errors.Wrapf(err, "failed to get origin metadata")
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
	return mm.writeFileMetaData(ctx, originMetaData)
}

// appendPieceMetaDataToFile append piece meta info to storage
func (mm *metaDataManager) appendPieceMetaDataToFile(ctx context.Context, taskId string,
	record *pieceMetaRecord) error {
	pieceMeta := getPieceMetaValue(record)
	mm.pieceMetaLocker.GetLock(taskId, false)
	defer mm.pieceMetaLocker.ReleaseLock(taskId, false)
	// write to the storage
	return mm.fileStore.AppendPieceMetaDataBytes(ctx, taskId, []byte(pieceMeta+"\n"))
}

// writePieceMetaRecords writes the piece meta data to storage.
func (mm *metaDataManager) appendPieceMetaIntegrityData(ctx context.Context, taskId, fileMD5 string) error {
	mm.pieceMetaLocker.GetLock(taskId, false)
	defer mm.pieceMetaLocker.ReleaseLock(taskId, false)
	pieceMetaRecords, err := mm.readPieceMetaRecordsWithoutCheck(ctx, taskId)
	if err != nil {
		return errors.Wrapf(err, "failed to read piece meta records")
	}
	pieceMetaStrs := make([]string, 0, len(pieceMetaRecords)+2)
	for _, record := range pieceMetaRecords {
		pieceMetaStrs = append(pieceMetaStrs, getPieceMetaValue(record))
	}
	pieceMetaStrs = append(pieceMetaStrs, fileMD5)
	pieceStr := strings.Join([]string{fileMD5, digest.Sha1(pieceMetaStrs)}, "\n")
	return mm.fileStore.AppendPieceMetaDataBytes(ctx, taskId, []byte(pieceStr))
}

// readAndCheckPieceMetaRecords reads pieceMetaRecords from storage and check data integrity by the md5 file of the TaskId
func (mm *metaDataManager) readAndCheckPieceMetaRecords(ctx context.Context, taskId,
	fileMD5 string) ([]*pieceMetaRecord, error) {
	mm.pieceMetaLocker.GetLock(taskId, true)
	defer mm.pieceMetaLocker.ReleaseLock(taskId, true)

	bytes, err := mm.fileStore.ReadPieceMetaBytes(ctx, taskId)
	if err != nil {
		return nil, err
	}
	pieceMetaRecords := strings.Split(strings.TrimSpace(string(bytes)), "\n")
	piecesLength := len(pieceMetaRecords)
	if piecesLength < 3 {
		return nil, errors.Errorf("piece meta file content line count is invalid, at least 3, but actually only %d", piecesLength)
	}
	// validate the fileMD5
	realFileMD5 := pieceMetaRecords[piecesLength-2]
	if realFileMD5 != fileMD5 {
		return nil, errors.Errorf("failed to check the fileMD5, expected: %s, real: %s", fileMD5, realFileMD5)
	}
	piecesWithoutSha1Value := pieceMetaRecords[:piecesLength-1]
	expectedSha1Value := digest.Sha1(piecesWithoutSha1Value)
	realSha1Value := pieceMetaRecords[piecesLength-1]
	if expectedSha1Value != realSha1Value {
		return nil, errors.Errorf("failed to validate the SHA-1 checksum of piece meta records, expected: %s, " +
			"real: %s", expectedSha1Value, realSha1Value)
	}
	var result = make([]*pieceMetaRecord, 0, piecesLength-2)
	for _, pieceStr := range pieceMetaRecords[:piecesLength-2] {
		record, err := parsePieceMetaRecord(pieceStr, FieldSeparator)
		if err != nil {
			return nil, errors.Wrap(err, "failed to get piece meta record")
		}
		result = append(result, record)
	}
	return result, nil
}

// readPieceMetaRecordsWithoutCheck reads pieceMetaRecords from storage and without check data integrity
func (mm *metaDataManager) readPieceMetaRecordsWithoutCheck(ctx context.Context, taskId string) ([]*pieceMetaRecord, error) {
	bytes, err := mm.fileStore.ReadPieceMetaBytes(ctx, taskId)
	if err != nil {
		return nil, err
	}

	pieceMetaRecords := strings.Split(strings.TrimSpace(string(bytes)), "\n")
	if len(pieceMetaRecords) == 0 {
		return nil, dferrors.ErrDataNotFound
	}
	var result = make([]*pieceMetaRecord, 0, len(pieceMetaRecords))
	for _, pieceRecord := range pieceMetaRecords {
		if len(strings.Split(pieceRecord, FieldSeparator)) == 0 {
			// 如果是签名或者文件md5则忽略
			continue
		}
		record, err := parsePieceMetaRecord(pieceRecord, FieldSeparator)
		if err != nil {
			return nil, err
		} else {
			result = append(result, record)
		}
	}
	return result, nil
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
	return digest.Sha1(pieceMd5),nil
}

