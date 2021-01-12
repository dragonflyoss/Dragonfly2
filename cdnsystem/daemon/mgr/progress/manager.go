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
	"container/list"
	"context"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/daemon/mgr"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
	"github.com/dragonflyoss/Dragonfly2/pkg/dferrors"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
	"github.com/dragonflyoss/Dragonfly2/pkg/struct/syncmap"
	"github.com/pkg/errors"
	"sync"
)

type Manager struct {
	seedSubscribers      *syncmap.SyncMap
	taskPieceMetaRecords *syncmap.SyncMap
	buffer               int
}

func NewManager() *Manager {
	return &Manager{
		seedSubscribers:      syncmap.NewSyncMap(),
		taskPieceMetaRecords: syncmap.NewSyncMap(),
		buffer:               64,
	}
}

func (pm *Manager) InitSeedProgress(ctx context.Context, taskID string) error {
	chanList := list.New()
	pieceRecords := syncmap.NewSyncMap()
	if err := pm.seedSubscribers.Add(taskID, chanList); err != nil {
		return errors.Wrap(err, "failed to add seed subscribers map")
	}
	if err := pm.taskPieceMetaRecords.Add(taskID, pieceRecords); err != nil {
		return errors.Wrap(err, "failed to add task piece meta records map")
	}
	return nil
}

func (pm *Manager) WatchSeedProgress(ctx context.Context, taskID string, taskMgr mgr.SeedTaskMgr) (<-chan *types.SeedPiece, error) {
	logger.Debugf("watch seed progress begin for taskID:%s", taskID)
	chanList, err := pm.seedSubscribers.GetAsList(taskID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get seed subscribers")
	}
	pieceMetaDataRecords, err := pm.getPieceMetaRecordsByTaskID(taskID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get piece meta records by taskId")
	}
	ch := make(chan *types.SeedPiece, pm.buffer)
	chanList.PushBack(ch)
	go func(seedCh chan *types.SeedPiece) {
		for _, pieceMetaRecord := range pieceMetaDataRecords {
			logger.Debugf("seed piece meta record %s", pieceMetaRecord)
			seedCh <- pieceMetaRecord
		}
	}(ch)
	return ch, nil
}

func (pm *Manager) UnWatchSeedProgress(sub chan *types.SeedPiece, taskID string) error {
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

// Publish publish seedPiece
func (pm *Manager) PublishPiece(taskID string, record *types.SeedPiece) error {
	logger.Debugf("seed piece meta record %s", record)
	err := pm.setPieceMetaRecord(taskID, record)
	if err != nil {
		errors.Wrap(err, "failed to set piece meta record")
	}
	chanList, err := pm.seedSubscribers.GetAsList(taskID)
	if err != nil {
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

func (pm *Manager) PublishTask(taskID string, record *types.SeedPiece) error {
	logger.Debugf("seed task record %s", record)
	chanList, err := pm.seedSubscribers.GetAsList(taskID)
	if err != nil {
		return errors.Wrap(err, "failed to get seed subscribers")
	}
	var wg sync.WaitGroup
	for e := chanList.Front(); e != nil; e = e.Next() {
		wg.Add(1)
		sub := e.Value.(chan *types.SeedPiece)
		go func(sub chan *types.SeedPiece, record *types.SeedPiece) {
			defer wg.Done()
			sub <- record
			pm.UnWatchSeedProgress(sub, taskID)
		}(sub, record)
	}
	wg.Wait()
	return nil
}

func (pm *Manager) Clear(taskID string) error {
	chanList, err := pm.seedSubscribers.GetAsList(taskID)
	if err != nil && !dferrors.IsDataNotFound(err) {
		return errors.Wrap(err, "failed to get seed subscribers")
	}
	if chanList != nil {
		var next *list.Element
		for e := chanList.Front(); e != nil; e = next {
			next = e.Next()
			sub := e.Value.(chan *types.SeedPiece)
			close(sub)
			chanList.Remove(e)
		}
	}
	err = pm.seedSubscribers.Remove(taskID)
	if err != nil && !dferrors.IsDataNotFound(err) {
		return errors.Wrap(err, "failed to clear seed subscribes")
	}
	err = pm.taskPieceMetaRecords.Remove(taskID)
	if err != nil && !dferrors.IsDataNotFound(err) {
		return errors.Wrap(err, "failed to clear piece meta records")
	}
	return nil
}

func (pm *Manager) GetPieceMetaRecordsByTaskID(taskID string) (records []*types.SeedPiece, err error) {
	return pm.getPieceMetaRecordsByTaskID(taskID)
}
