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

package localcdn

import (
	"github.com/dragonflyoss/Dragonfly2/pkg/dferrors"
	"github.com/dragonflyoss/Dragonfly2/pkg/struct/syncmap"
	"github.com/pkg/errors"
	"sort"
	"strconv"
)

type SeedPieceMetaDataManager struct {
	taskPieceMetaRecords *syncmap.SyncMap
}

func newPieceMetaDataMgr() *SeedPieceMetaDataManager {
	return &SeedPieceMetaDataManager{
		taskPieceMetaRecords: syncmap.NewSyncMap(),
	}
}

// getPieceMetaRecord
func (pmm *SeedPieceMetaDataManager) getPieceMetaRecord(taskID string, pieceNum int) (pieceMetaRecord, error) {
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
func (pmm *SeedPieceMetaDataManager) setPieceMetaRecord(taskID string, pieceMetaRecord pieceMetaRecord) error {
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
func (pmm *SeedPieceMetaDataManager) getPieceMetaRecordsByTaskID(taskID string) (pieceMetaRecords []pieceMetaRecord, err error) {
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

func (pmm *SeedPieceMetaDataManager) removePieceMetaRecordsByTaskID(taskID string) error {
	return pmm.taskPieceMetaRecords.Remove(taskID)
}
