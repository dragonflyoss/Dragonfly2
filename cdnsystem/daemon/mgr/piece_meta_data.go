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

package mgr

import (
	"github.com/dragonflyoss/Dragonfly2/pkg/dferrors"
	"github.com/dragonflyoss/Dragonfly2/pkg/struct/syncmap"
	"github.com/pkg/errors"
	"sort"
	"strconv"
)

type pieceMetaRecord struct {
	PieceNum int32  `json:"pieceNum"`
	PieceLen int32  `json:"pieceLen"` // 下载存储的真实长度
	Md5      string `json:"md5"`
	Range    string `json:"range"` // 下载存储到磁盘的range，不一定是origin source的range
	Offset   int64  `json:"offset"`
}


type seedPieceMetaDataManager struct {
	taskPieceMetaRecords *syncmap.SyncMap
}

func NewPieceMetaDataMgr() *seedPieceMetaDataManager {
	return &seedPieceMetaDataManager{
		taskPieceMetaRecords: syncmap.NewSyncMap(),
	}
}

// getPieceMetaRecord
func (pmm *seedPieceMetaDataManager) getPieceMetaRecord(taskID string, pieceNum int) (pieceMetaRecord, error) {
	pieceMetaRecords, err := pmm.taskPieceMetaRecords.GetAsMap(taskID)
	if err != nil {
		return pieceMetaRecord{}, errors.Wrapf(err, "taskID:%s, failed to get pieceMetaRecords", taskID)
	}
	v, err := pieceMetaRecords.Get(strconv.Itoa(pieceNum))
	if err != nil {
		return pieceMetaRecord{}, errors.Wrapf(err, "taskID:%s, failed to get pieceCount(%d)piece meta record", taskID, pieceNum)
	}

	if value, ok := v.(pieceMetaRecord); ok {
		return value, nil
	}
	return pieceMetaRecord{}, errors.Wrapf(dferrors.ErrConvertFailed, "taskID:%s, failed to convert piece count(%d) from map with value %v", taskID, pieceNum, v)
}

// setPieceMetaRecord
func (pmm *seedPieceMetaDataManager) setPieceMetaRecord(taskID string, pieceMetaRecord pieceMetaRecord) error {
	pieceRecords, err := pmm.taskPieceMetaRecords.GetAsMap(taskID)
	if err != nil && !dferrors.IsDataNotFound(err) {
		return err
	}
	if pieceRecords == nil {
		pieceRecords = syncmap.NewSyncMap()
		pmm.taskPieceMetaRecords.Add(taskID, pieceRecords)
	}

	return pieceRecords.Add(strconv.Itoa(int(pieceMetaRecord.PieceNum)), pieceMetaRecord)
}

// getPieceMetaRecordsByTaskID
func (pmm *seedPieceMetaDataManager) getPieceMetaRecordsByTaskID(taskID string) (pieceMetaRecords []pieceMetaRecord, err error) {
	pieceMetaRecordMap, err := pmm.taskPieceMetaRecords.GetAsMap(taskID)
	if err != nil {
		return nil, errors.Wrapf(err, "taskID:%s, failed to get piece meta records", taskID)
	}
	pieceNums := pieceMetaRecordMap.ListKeyAsIntSlice()
	sort.Ints(pieceNums)
	for i := 0; i < len(pieceNums); i++ {
		pieceMetaRecord, err := pmm.getPieceMetaRecord(taskID, pieceNums[i])
		if err != nil {
			return nil, errors.Wrapf(err, "taskID:%s, failed to get piece meta record", taskID)
		}
		pieceMetaRecords = append(pieceMetaRecords, pieceMetaRecord)
	}
	return pieceMetaRecords, nil
}

func (pmm *seedPieceMetaDataManager) removePieceMetaRecordsByTaskID(taskID string) error {
	return pmm.taskPieceMetaRecords.Remove(taskID)
}
