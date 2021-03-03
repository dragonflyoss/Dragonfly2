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
	"encoding/json"
	"strings"

	"d7y.io/dragonfly/v2/cdnsystem/store"
	"d7y.io/dragonfly/v2/cdnsystem/types"
	"d7y.io/dragonfly/v2/cdnsystem/util"
	"d7y.io/dragonfly/v2/pkg/dferrors"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/util/digestutils"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
	"github.com/pkg/errors"
)

// fileMetaData
type fileMetaData struct {
	TaskID          string            `json:"taskID"`
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

// fileMetaDataManager manages the meta file and piece meta file of each taskID.
type metaDataManager struct {
	fileStore       *store.Store
	fileMetaLocker  *util.LockerPool
	pieceMetaLocker *util.LockerPool
}

func newFileMetaDataManager(store *store.Store) *metaDataManager {
	return &metaDataManager{
		fileStore:       store,
		fileMetaLocker:  util.NewLockerPool(),
		pieceMetaLocker: util.NewLockerPool(),
	}
}

// writeFileMetaDataByTask stores the metadata of task by task to storage.
func (mm *metaDataManager) writeFileMetaDataByTask(ctx context.Context, task *types.SeedTask) (*fileMetaData, error) {
	metaData := &fileMetaData{
		TaskID:          task.TaskID,
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

// writeFileMetaData stores the metadata of taskID to storage.
func (mm *metaDataManager) writeFileMetaData(ctx context.Context, metaData *fileMetaData) error {
	data, err := json.Marshal(metaData)
	if err != nil {
		return errors.Wrapf(err, "failed to marshal metadata")
	}
	return mm.fileStore.PutBytes(ctx, getTaskMetaDataRawFunc(metaData.TaskID), data)
}

// readFileMetaData read task metadata information according to taskId
func (mm *metaDataManager) readFileMetaData(ctx context.Context, taskID string) (*fileMetaData, error) {
	bytes, err := mm.fileStore.GetBytes(ctx, getTaskMetaDataRawFunc(taskID))
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get metadata bytes")
	}

	metaData := &fileMetaData{}
	if err := json.Unmarshal(bytes, metaData); err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal metadata bytes")
	}
	logger.WithTaskID(taskID).Debugf("success to read metadata: %+v ", metaData)
	return metaData, nil
}

// updateAccessTime update access and interval
func (mm *metaDataManager) updateAccessTime(ctx context.Context, taskID string, accessTime int64) error {
	mm.fileMetaLocker.GetLock(taskID, false)
	defer mm.fileMetaLocker.ReleaseLock(taskID, false)

	originMetaData, err := mm.readFileMetaData(ctx, taskID)
	if err != nil {
		return errors.Wrapf(err, "failed to get origin metaData")
	}
	// access interval
	interval := accessTime - originMetaData.AccessTime
	originMetaData.Interval = interval
	if interval <= 0 {
		logger.WithTaskID(taskID).Warnf("file hit interval:%d", interval)
		originMetaData.Interval = 0
	}

	originMetaData.AccessTime = accessTime

	return mm.writeFileMetaData(ctx, originMetaData)
}

func (mm *metaDataManager) updateExpireInfo(ctx context.Context, taskID string, expireInfo map[string]string) error {
	mm.fileMetaLocker.GetLock(taskID, false)
	defer mm.fileMetaLocker.ReleaseLock(taskID, false)

	originMetaData, err := mm.readFileMetaData(ctx, taskID)
	if err != nil {
		return err
	}

	originMetaData.ExpireInfo = expireInfo

	return mm.writeFileMetaData(ctx, originMetaData)
}

func (mm *metaDataManager) updateStatusAndResult(ctx context.Context, taskID string, metaData *fileMetaData) error {
	mm.fileMetaLocker.GetLock(taskID, false)
	defer mm.fileMetaLocker.ReleaseLock(taskID, false)

	originMetaData, err := mm.readFileMetaData(ctx, taskID)
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
func (pmm *metaDataManager) appendPieceMetaDataToFile(ctx context.Context, taskID string, record *pieceMetaRecord) error {
	pieceMeta := getPieceMetaValue(record)
	pmm.pieceMetaLocker.GetLock(taskID, false)
	defer pmm.pieceMetaLocker.ReleaseLock(taskID, false)
	// write to the storage
	return pmm.fileStore.AppendBytes(ctx, getPieceMetaDataRawFunc(taskID), []byte(pieceMeta+"\n"))
}

// writePieceMetaRecords writes the piece meta data to storage.
func (pmm *metaDataManager) appendPieceMetaIntegrityData(ctx context.Context, taskID, fileMD5 string) error {
	pmm.pieceMetaLocker.GetLock(taskID, false)
	defer pmm.pieceMetaLocker.ReleaseLock(taskID, false)
	pieceMetaRecords, err := pmm.readPieceMetaRecordsWithoutCheck(ctx, taskID)
	if err != nil {
		return errors.Wrapf(err, "failed to read piece meta records")
	}
	pieceMetaStrs := make([]string, 0, len(pieceMetaRecords)+2)
	for _, record := range pieceMetaRecords {
		pieceMetaStrs = append(pieceMetaStrs, getPieceMetaValue(record))
	}
	pieceMetaStrs = append(pieceMetaStrs, fileMD5)
	pieceStr := strings.Join([]string{fileMD5, digestutils.Sha256(pieceMetaStrs...)}, "\n")
	return pmm.fileStore.AppendBytes(ctx, getPieceMetaDataRawFunc(taskID), []byte(pieceStr))
}

// readAndCheckPieceMetaRecords reads pieceMetaRecords from storage and check data integrity by the md5 file of the taskID
func (pmm *metaDataManager) readAndCheckPieceMetaRecords(ctx context.Context, taskID, fileMD5 string) ([]*pieceMetaRecord, error) {
	pmm.pieceMetaLocker.GetLock(taskID, true)
	defer pmm.pieceMetaLocker.ReleaseLock(taskID, true)

	bytes, err := pmm.fileStore.GetBytes(ctx, getPieceMetaDataRawFunc(taskID))
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
	expectedSha1Value := digestutils.Sha256(piecesWithoutSha1Value...)
	realSha1Value := pieceMetaRecords[piecesLength-1]
	if expectedSha1Value != realSha1Value {
		return nil, errors.Errorf("failed to validate the SHA-1 checksum of piece meta records, expected: %s, real: %s", expectedSha1Value, realSha1Value)
	}
	var result = make([]*pieceMetaRecord, 0, piecesLength-2)
	for _, pieceStr := range pieceMetaRecords[:piecesLength-2] {
		record, err := getPieceMetaRecord(pieceStr)
		if err != nil {
			return nil, errors.Wrap(err, "failed to get piece meta record")
		}
		result = append(result, record)
	}
	return result, nil
}

// readPieceMetaRecordsWithoutCheck reads pieceMetaRecords from storage and without check data integrity
func (pmm *metaDataManager) readPieceMetaRecordsWithoutCheck(ctx context.Context, taskID string) ([]*pieceMetaRecord, error) {
	bytes, err := pmm.fileStore.GetBytes(ctx, getPieceMetaDataRawFunc(taskID))
	if err != nil {
		return nil, err
	}

	pieceMetaRecords := strings.Split(strings.TrimSpace(string(bytes)), "\n")
	if len(pieceMetaRecords) == 0 {
		return nil, dferrors.ErrDataNotFound
	}
	var result = make([]*pieceMetaRecord, 0, len(pieceMetaRecords))
	for _, pieceRecord := range pieceMetaRecords {
		record, err := getPieceMetaRecord(pieceRecord)
		if err != nil {
			return nil, err
		} else {
			result = append(result, record)
		}
	}
	return result, nil
}

func (pm *metaDataManager) getPieceMd5Sign(ctx context.Context, taskID string) (md5Sign string, err error) {
	pieceMetaRecords, err := pm.readPieceMetaRecordsWithoutCheck(ctx, taskID)
	if err != nil {
		return "", errors.Wrapf(err, "failed to read piece meta file")
	}
	var pieceMd5 []string
	for _, piece := range pieceMetaRecords {
		pieceMd5 = append(pieceMd5, piece.Md5)
	}
	return digestutils.Sha256(pieceMd5...),nil
}

