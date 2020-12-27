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
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/daemon/mgr/piece"
	"github.com/dragonflyoss/Dragonfly2/pkg/dferrors"
	"github.com/dragonflyoss/Dragonfly2/pkg/struct/syncmap"
	"github.com/pkg/errors"
	"sort"
	"strconv"
)

type seedPieceMetaDataManager struct {
	taskPieceMetaRecords *syncmap.SyncMap
	publisher            *piece.SeedPiecePublisher
}

func NewPieceMetaDataMgr(publisher *piece.SeedPiecePublisher) *seedPieceMetaDataManager {
	return &seedPieceMetaDataManager{
		taskPieceMetaRecords: syncmap.NewSyncMap(),
		publisher:            publisher,
	}
}

// getPieceMetaRecord
func (pmm *seedPieceMetaDataManager) getPieceMetaRecord(taskID string, pieceNum int) (PieceMetaRecord, error) {
	pieceMetaRecords, err := pmm.taskPieceMetaRecords.GetAsMap(taskID)
	if err != nil {
		return PieceMetaRecord{}, errors.Wrapf(err, "taskID:%s, failed to get pieceMetaRecords", taskID)
	}
	v, err := pieceMetaRecords.Get(strconv.Itoa(pieceNum))
	if err != nil {
		return PieceMetaRecord{}, errors.Wrapf(err, "taskID:%s, failed to get pieceCount(%d)piece meta record", taskID, pieceNum)
	}

	if value, ok := v.(PieceMetaRecord); ok {
		return value, nil
	}
	return PieceMetaRecord{}, errors.Wrapf(dferrors.ErrConvertFailed, "taskID:%s, failed to convert piece count(%d) from map with value %v", taskID, pieceNum, v)
}

// setPieceMetaRecord
func (pmm *seedPieceMetaDataManager) setPieceMetaRecord(taskID string, pieceMetaRecord PieceMetaRecord) error {
	pieceRecords, err := pmm.taskPieceMetaRecords.GetAsMap(taskID)
	if err != nil && !dferrors.IsDataNotFound(err) {
		return err
	}
	if pieceRecords == nil {
		pieceRecords = syncmap.NewSyncMap()
		pmm.taskPieceMetaRecords.Add(taskID, pieceRecords)
	}

	pieceRecords.Add(strconv.Itoa(int(pieceMetaRecord.PieceNum)), pieceMetaRecord)
	pmm.publisher.Publish(taskID, pieceMetaRecord)
}

// getPieceMetaRecordsByTaskID
func (pmm *seedPieceMetaDataManager) getPieceMetaRecordsByTaskID(taskID string) (pieceMetaRecords []PieceMetaRecord, err error) {
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
