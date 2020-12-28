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
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/store"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/util"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
	"github.com/dragonflyoss/Dragonfly2/pkg/digest"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/stringutils"
	"github.com/pkg/errors"
	"strings"
)

// fileMetaData
type fileMetaData struct {
	TaskID          string            `json:"taskID"`
	URL             string            `json:"url"`
	PieceSize       int32             `json:"pieceSize"`
	SourceFileLen   int64             `json:"sourceFileLen"`
	AccessTime      int64             `json:"accessTime"`
	Interval        int64             `json:"interval"`
	CdnFileLength   int64             `json:"cdnFileLength"`
	SourceMd5       string            `json:"sourceRealMd5"`
	ExpireInfo      map[string]string `json:"expireInfo"`
	Finish          bool              `json:"finish"`
	Success         bool              `json:"success"`
	TotalPieceCount int               `json:"totalPieceCount"`
}

// pieceMetaData
type pieceMetaData struct {
	PieceMetaRecords []pieceMetaRecord `json:"pieceMetaRecords"`
	FileMd5          string            `json:"fileMd5"`
	Sha1Value        string            `json:"sha1Value"`
}

// pieceMetaRecord
type pieceMetaRecord struct {
	PieceNum   int32  `json:"pieceNum"`
	PieceLen   int32  `json:"pieceLen"` // 下载存储的真实长度
	Md5        string `json:"md5"`
	Range      string `json:"range"` // 下载存储到磁盘的range，不一定是origin source的range
	Offset     uint64  `json:"offset"`
	PieceStyle int32  `json:"pieceStyle"`
}

// fileMetaDataManager manages the meta file and piece meta file of each taskID.
type metaDataManager struct {
	fileStore *store.Store
	locker    *util.LockerPool
}

func newFileMetaDataManager(store *store.Store) *metaDataManager {
	return &metaDataManager{
		fileStore: store,
		locker:    util.NewLockerPool(),
	}
}

// writeFileMetaDataByTask stores the metadata of task by task to storage.
func (mm *metaDataManager) writeFileMetaDataByTask(ctx context.Context, task *types.SeedTask) (*fileMetaData, error) {
	metaData := &fileMetaData{
		TaskID:        task.TaskID,
		URL:           task.Url,
		PieceSize:     task.PieceSize,
		SourceFileLen: task.SourceFileLength,
		AccessTime:    getCurrentTimeMillisFunc(),
		CdnFileLength: task.CdnFileLength,
	}

	if err := mm.writeFileMetaData(ctx, metaData); err != nil {
		return nil, err
	}

	return metaData, nil
}

// writeFileMetaData stores the metadata of task.ID to storage.
func (mm *metaDataManager) writeFileMetaData(ctx context.Context, metaData *fileMetaData) error {
	data, err := json.Marshal(metaData)
	if err != nil {
		return errors.Wrapf(err, "failed to marshal metadata")
	}

	return mm.fileStore.PutBytes(ctx, getTaskMetaDataRawFunc(metaData.TaskID), data)
}

// readFileMetaData returns the fileMetaData info according to the taskID.
func (mm *metaDataManager) readFileMetaData(ctx context.Context, taskID string) (*fileMetaData, error) {
	bytes, err := mm.fileStore.GetBytes(ctx, getTaskMetaDataRawFunc(taskID))
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get metadata bytes")
	}

	metaData := &fileMetaData{}
	if err := json.Unmarshal(bytes, metaData); err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal metadata bytes")
	}
	logger.Debugf("success to read metadata: %+v for taskID: %s", metaData, taskID)

	return metaData, nil
}

// updateAccessTime update access and interval
func (mm *metaDataManager) updateAccessTime(ctx context.Context, taskID string, accessTime int64) error {
	mm.locker.GetLock(taskID, false)
	defer mm.locker.ReleaseLock(taskID, false)

	originMetaData, err := mm.readFileMetaData(ctx, taskID)
	if err != nil {
		return errors.Wrapf(err, "failed to get origin metaData")
	}
	// access interval
	interval := accessTime - originMetaData.AccessTime
	originMetaData.Interval = interval
	if interval <= 0 {
		logger.Warnf("taskId:%s file hit interval:%d", taskID, interval)
		originMetaData.Interval = 0
	}

	originMetaData.AccessTime = accessTime

	return mm.writeFileMetaData(ctx, originMetaData)
}

func (mm *metaDataManager) updateExpireInfo(ctx context.Context, taskID string, expireInfo map[string]string) error {
	mm.locker.GetLock(taskID, false)
	defer mm.locker.ReleaseLock(taskID, false)

	originMetaData, err := mm.readFileMetaData(ctx, taskID)
	if err != nil {
		return err
	}

	originMetaData.ExpireInfo = expireInfo

	return mm.writeFileMetaData(ctx, originMetaData)
}

func (mm *metaDataManager) updateStatusAndResult(ctx context.Context, taskID string, metaData *fileMetaData) error {
	mm.locker.GetLock(taskID, false)
	defer mm.locker.ReleaseLock(taskID, false)

	originMetaData, err := mm.readFileMetaData(ctx, taskID)
	if err != nil {
		return errors.Wrapf(err, "failed to get origin metadata")
	}

	originMetaData.Finish = metaData.Finish
	originMetaData.Success = metaData.Success
	if originMetaData.Success {
		originMetaData.CdnFileLength = metaData.CdnFileLength
		if !stringutils.IsEmptyStr(metaData.SourceMd5) {
			originMetaData.SourceMd5 = metaData.SourceMd5
		}
	}

	return mm.writeFileMetaData(ctx, originMetaData)
}

// writePieceMetaRecords writes the piece meta data to storage.
func (pmm *metaDataManager) writePieceMetaRecords(ctx context.Context, taskID, fileMD5 string, pieceMetaRecords []pieceMetaRecord) error {
	pmm.locker.GetLock(taskID, false)
	defer pmm.locker.ReleaseLock(taskID, false)

	if len(pieceMetaRecords) == 0 {
		logger.Warnf("failed to write empty pieceMetaRecords for taskID: %s", taskID)
		return nil
	}
	var pieceMetaStrs []string
	for _, record := range pieceMetaRecords {
		pieceMetaStrs = append(pieceMetaStrs, getPieceMetaValue(record))
	}
	pieceMetaStrs = append(pieceMetaStrs, fileMD5)
	pieceMetaStrs = append(pieceMetaStrs, digest.Sha1(pieceMetaStrs))
	pieceStr := strings.Join(pieceMetaStrs, "\n")
	return pmm.fileStore.PutBytes(ctx, getPieceMetaDataRawFunc(taskID), []byte(pieceStr))
}

// readPieceMetaDatas reads the md5 file of the taskID and returns the pieceMD5s.
func (pmm *metaDataManager) readAndCheckPieceMetaRecords(ctx context.Context, taskID, fileMD5 string) ([]pieceMetaRecord, error) {
	pmm.locker.GetLock(taskID, true)
	defer pmm.locker.ReleaseLock(taskID, true)

	bytes, err := pmm.fileStore.GetBytes(ctx, getPieceMetaDataRawFunc(taskID))
	if err != nil {
		return nil, err
	}
	pieceMetaRecords := strings.Split(strings.TrimSpace(string(bytes)), "\n")
	piecesLength := len(pieceMetaRecords)
	if piecesLength < 3 {
		return nil, errors.Errorf("piece meta file content line count is invalid, line count:%d", piecesLength)
	}
	piecesWithoutSha1Value := pieceMetaRecords[:piecesLength-1]
	expectedSha1Value := digest.Sha1(piecesWithoutSha1Value)
	realSha1Value := pieceMetaRecords[piecesLength-1]
	if expectedSha1Value != realSha1Value {
		return nil, errors.Errorf("failed to validate the SHA-1 checksum of piece meta records, expected: %s, real: %s", expectedSha1Value, realSha1Value)
	}

	// validate the fileMD5
	realFileMD5 := pieceMetaRecords[piecesLength-2]
	if realFileMD5 != fileMD5 {
		return nil, errors.Errorf("failed to validate the fileMD5, expected: %s, real: %s", fileMD5, realFileMD5)
	}
	var result = make([]pieceMetaRecord, 0, piecesLength-2)
	for _, pieceStr := range pieceMetaRecords[:piecesLength-2] {
		result = append(result, getPieceMetaRecord(pieceStr))
	}
	return result, nil
}

// readPieceMetaDatas reads the md5 file of the taskID and returns the pieceMD5s.
func (pmm *metaDataManager) readWithoutCheckPieceMetaRecords(ctx context.Context, taskID string) ([]pieceMetaRecord, error) {
	pmm.locker.GetLock(taskID, true)
	defer pmm.locker.ReleaseLock(taskID, true)

	bytes, err := pmm.fileStore.GetBytes(ctx, getPieceMetaDataRawFunc(taskID))
	if err != nil {
		return nil, err
	}

	pieceMetaRecords := strings.Split(strings.TrimSpace(string(bytes)), "\n")
	if len(pieceMetaRecords) == 0 {
		return nil, nil
	}
	var result = make([]pieceMetaRecord, len(pieceMetaRecords))
	for _, pieceRecord := range pieceMetaRecords {
		result = append(result, getPieceMetaRecord(pieceRecord))
	}
	return result, nil
}
