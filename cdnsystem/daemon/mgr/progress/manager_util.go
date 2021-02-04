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

package progress

import (
	"github.com/dragonflyoss/Dragonfly/v2/cdnsystem/types"
	"github.com/pkg/errors"
	"sort"
	"strconv"
)

// setPieceMetaRecord
func (pm *Manager) setPieceMetaRecord(taskID string, record *types.SeedPiece) error {
	pieceRecords, err := pm.taskPieceMetaRecords.GetAsMap(taskID)
	if err != nil {
		return err
	}
	return pieceRecords.Add(strconv.Itoa(int(record.PieceNum)), record)
}

// getPieceMetaRecordsByTaskID
func (pm *Manager) getPieceMetaRecordsByTaskID(taskID string) (records []*types.SeedPiece, err error) {
	pieceRecords, err := pm.taskPieceMetaRecords.GetAsMap(taskID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get piece meta records")
	}
	pieceNums := pieceRecords.ListKeyAsIntSlice()
	sort.Ints(pieceNums)
	for i := 0; i < len(pieceNums); i++ {
		v, _ := pieceRecords.Get(strconv.Itoa(pieceNums[i]))
		if value, ok := v.(*types.SeedPiece); ok {
			records = append(records, value)
		}
	}
	return records, nil
}

func (pm *Manager) unWatchSeedProgress(sub chan *types.SeedPiece, taskID string) error {
	chanList, err := pm.seedSubscribers.GetAsList(taskID)
	if err != nil {
		return errors.Wrap(err, "failed to get seed subscribers")
	}
	for e := chanList.Front(); e != nil; e = e.Next() {
		if e.Value.(chan *types.SeedPiece) == sub {
			chanList.Remove(e)
			break
		}
	}
	close(sub)
	return nil
}


