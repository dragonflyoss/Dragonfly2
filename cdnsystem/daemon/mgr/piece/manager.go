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

package piece

import (
	"container/list"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/util"
	"github.com/dragonflyoss/Dragonfly2/pkg/dferrors"
	"github.com/dragonflyoss/Dragonfly2/pkg/struct/syncmap"
	"github.com/pkg/errors"
	"sort"
	"strconv"
	"sync"
)

type Manager struct {
	seedSubscribers      *syncmap.SyncMap
	taskPieceMetaRecords *syncmap.SyncMap
	buffer               int
	m                    *util.LockerPool
}

func NewManager() *Manager {
	return &Manager{
		seedSubscribers:      syncmap.NewSyncMap(),
		taskPieceMetaRecords: syncmap.NewSyncMap(),
		buffer:               64,
		m:                    util.NewLockerPool(),
	}
}

// SubscribeTask subscribe task's piece seed
func (pm *Manager) WatchSeedTask(taskID string) (<-chan *types.SeedPiece, error) {
	pm.m.GetLock(taskID, true)
	defer pm.m.ReleaseLock(taskID, true)
	chanList, err := pm.seedSubscribers.GetAsList(taskID)
	if err != nil && !dferrors.IsDataNotFound(err) {
		return nil, errors.Wrap(err, "failed to get seed subscribers")
	}
	if dferrors.IsDataNotFound(err) {
		pm.m.GetLock(taskID, false)
		pm.seedSubscribers.GetAsList(taskID)
		chanList = list.New()
		pm.seedSubscribers.Add(taskID, chanList)
	}
	ch := make(chan *types.SeedPiece, pm.buffer)
	chanList.PushBack(ch)
	pieceMetaDataRecords, err := pm.getPieceMetaRecordsByTaskID(taskID)
	if err != nil && !dferrors.IsDataNotFound(err) {
		return nil, errors.Wrap(err, "failed to get piece meta records by taskId")
	}
	for _, pieceMetaRecord := range pieceMetaDataRecords {
		ch <- pieceMetaRecord
	}
	return ch, nil
}

func (pm *Manager) UnWatchTask(sub chan *types.SeedPiece, taskID string) error {
	pm.m.GetLock(taskID, false)
	defer pm.m.ReleaseLock(taskID, false)
	chanList, err := pm.seedSubscribers.GetAsList(taskID)
	if err != nil && !dferrors.IsDataNotFound(err) {
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

func (pm *Manager) GetPieceMetaRecordsByTaskID(taskID string) (records []*types.SeedPiece, err error) {
	return pm.getPieceMetaRecordsByTaskID(taskID)
}

// Publish publish seedPiece
func (pm *Manager) PublishPiece(taskID string, record *types.SeedPiece) error {
	pm.m.GetLock(taskID, false)
	defer pm.m.ReleaseLock(taskID, false)
	pm.setPieceMetaRecord(taskID, record)
	chanList, err := pm.seedSubscribers.GetAsList(taskID)
	if err != nil && !dferrors.IsDataNotFound(err) {
		return errors.Wrap(err, "failed to get seed subscribers")
	}
	var wg sync.WaitGroup
	for e := chanList.Front(); e != nil; e = e.Next() {
		wg.Add(1)
		sub := e.Value.(chan *types.SeedPiece)
		go func(sub chan *types.SeedPiece, record *types.SeedPiece) {
			defer wg.Done()
			sub <- record
		}(sub, record)
	}
	wg.Wait()
	return nil
}

// setPieceMetaRecord
func (pm *Manager) setPieceMetaRecord(taskID string, record *types.SeedPiece) error {
	pieceRecords, err := pm.taskPieceMetaRecords.GetAsMap(taskID)
	if err != nil && !dferrors.IsDataNotFound(err) {
		return err
	}
	if pieceRecords == nil {
		pieceRecords = syncmap.NewSyncMap()
		if err := pm.taskPieceMetaRecords.Add(taskID, pieceRecords); err != nil {
			return err
		}
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
		pieceMetaRecord, err := pm.getPieceMetaRecord(taskID, pieceNums[i])
		if err != nil {
			return nil, errors.Wrapf(err, "taskID:%s, failed to get piece meta record", taskID)
		}
		records = append(records, pieceMetaRecord)
	}
	return records, nil
}

func (p *Manager) Close(taskID string) error {
	p.m.GetLock(taskID, false)
	defer p.m.ReleaseLock(taskID, false)
	chanList, err := p.seedSubscribers.GetAsList(taskID)
	if err != nil && !dferrors.IsDataNotFound(err) {
		return errors.Wrap(err, "failed to get seed subscribers")
	}
	var wg sync.WaitGroup
	for e := chanList.Front(); e != nil; e = e.Next() {
		wg.Add(1)
		sub := e.Value.(chan *types.SeedPiece)
		go func(sub chan *types.SeedPiece, wg *sync.WaitGroup) {
			defer wg.Done()
			close(sub)
		}(sub, &wg)
	}
	wg.Wait()
	return p.removePieceMetaRecordsByTaskID(taskID)
}

// getpieceMetaRecord
func (pm *Manager) getPieceMetaRecord(taskID string, pieceNum int) (*types.SeedPiece, error) {
	pieceMetaRecords, err := pm.taskPieceMetaRecords.GetAsMap(taskID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get pieceMetaRecords")
	}
	v, err := pieceMetaRecords.Get(strconv.Itoa(pieceNum))
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get pieceCount(%d)piece meta record", pieceNum)
	}

	if value, ok := v.(*types.SeedPiece); ok {
		return value, nil
	}
	return nil, errors.Wrapf(dferrors.ErrConvertFailed, "failed to convert piece count(%d) from map with value %v", pieceNum, v)
}

func (pm *Manager) removePieceMetaRecordsByTaskID(taskID string) error {
	return pm.taskPieceMetaRecords.Remove(taskID)
}
