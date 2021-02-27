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
	"d7y.io/dragonfly/v2/cdnsystem/cdnerrors"
	"d7y.io/dragonfly/v2/cdnsystem/config"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/mgr"
	"d7y.io/dragonfly/v2/cdnsystem/types"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/struct/syncmap"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"sync"
)

func init() {
	// Ensure that Manager implements the SeedProgressMgr interface
	var manager *Manager = nil
	var _ mgr.SeedProgressMgr = manager
}

type metrics struct {
}

func newMetrics(register prometheus.Registerer) *metrics {
	return &metrics{

	}
}

type Manager struct {
	seedSubscribers      *syncmap.SyncMap
	taskPieceMetaRecords *syncmap.SyncMap
	progress             *syncmap.SyncMap
	buffer               int
	metrics              *metrics
	cfg                  *config.Config
}

func NewManager(cfg *config.Config, register prometheus.Registerer) (*Manager, error) {
	return &Manager{
		cfg:                  cfg,
		seedSubscribers:      syncmap.NewSyncMap(),
		taskPieceMetaRecords: syncmap.NewSyncMap(),
		progress:             syncmap.NewSyncMap(),
		metrics:              newMetrics(register),
	}, nil
}

func (pm *Manager) InitSeedProgress(ctx context.Context, taskID string) {
	if _, loaded := pm.seedSubscribers.LoadOrStore(taskID, list.New()); loaded {
		logger.Warnf("the task seedSubscribers already exist")
	}
	if _, loaded := pm.taskPieceMetaRecords.LoadOrStore(taskID, syncmap.NewSyncMap()); loaded {
		logger.Warnf("the task taskPieceMetaRecords already exist")
	}
}

func (pm *Manager) WatchSeedProgress(ctx context.Context, taskID string) (<-chan *types.SeedPiece, error) {
	logger.Debugf("watch seed progress begin for taskID:%s", taskID)
	//pm.mu.GetLock(taskID, true)
	//defer pm.mu.ReleaseLock(taskID, true)
	chanList, err := pm.seedSubscribers.GetAsList(taskID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get seed subscribers")
	}
	pieceMetaDataRecords, err := pm.getPieceMetaRecordsByTaskID(taskID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get piece meta records by taskId")
	}
	ch := make(chan *types.SeedPiece, pm.buffer)
	//task, _ := pm.progress.Get(taskID)
	//if task != nil {
	//	// seed progress has been done
	//	go func(seedCh chan *types.SeedPiece) {
	//		for _, pieceMetaRecord := range pieceMetaDataRecords {
	//			logger.Debugf("seed piece meta record %+v", pieceMetaRecord)
	//			seedCh <- pieceMetaRecord
	//		}
	//		// publish task info
	//		seedCh <- task.(*types.SeedPiece)
	//	}(ch)
	//} else {
	//	pm.mu.GetLock(taskID+"_push", false)
	chanList.PushBack(ch)
	//pm.mu.ReleaseLock(taskID+"_push", false)
	go func(seedCh chan *types.SeedPiece) {
		for _, pieceMetaRecord := range pieceMetaDataRecords {
			logger.Debugf("seed piece meta record %+v", pieceMetaRecord)
			seedCh <- pieceMetaRecord
		}
		if _, err := pm.progress.Get(taskID); err == nil {
			close(seedCh)
		}
	}(ch)
	//}
	return ch, nil
}

// Publish publish seedPiece
func (pm *Manager) PublishPiece(ctx context.Context, taskID string, record *types.SeedPiece) error {
	logger.Debugf("seed piece meta record %+v", record)
	//pm.mu.GetLock(taskID, true)
	err := pm.setPieceMetaRecord(taskID, record)
	//pm.mu.ReleaseLock(taskID, true)
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

func (pm *Manager) PublishTask(ctx context.Context, taskID string, task *types.SeedTask) error {
	logger.Debugf("seed task record %+v", task)
	//pm.mu.GetLock(taskID, true)
	//defer pm.mu.ReleaseLock(taskID, true)
	err := pm.progress.Add(taskID, task)
	//if err != nil {
	//	errors.Wrap(err, "failed to add task record")
	//}
	chanList, err := pm.seedSubscribers.GetAsList(taskID)
	if err != nil {
		return errors.Wrap(err, "failed to get seed subscribers")
	}
	var wg sync.WaitGroup
	// unwatch
	for e := chanList.Front(); e != nil; e = e.Next() {
		wg.Add(1)
		sub := e.Value.(chan *types.SeedPiece)
		go func(sub chan *types.SeedPiece, e *list.Element) {
			defer wg.Done()
			close(sub)
			chanList.Remove(e)
			//pm.unWatchSeedProgress(sub, taskID)
		}(sub, e)
	}
	wg.Wait()
	return nil
}

func (pm *Manager) Clear(ctx context.Context, taskID string) error {
	//pm.mu.GetLock(taskID, false)
	//defer pm.mu.ReleaseLock(taskID, false)
	chanList, err := pm.seedSubscribers.GetAsList(taskID)
	if err != nil && !cdnerrors.IsDataNotFound(err) {
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
		chanList = nil
	}
	err = pm.seedSubscribers.Remove(taskID)
	if err != nil && !cdnerrors.IsDataNotFound(err) {
		return errors.Wrap(err, "failed to clear seed subscribes")
	}
	err = pm.taskPieceMetaRecords.Remove(taskID)
	if err != nil && !cdnerrors.IsDataNotFound(err) {
		return errors.Wrap(err, "failed to clear piece meta records")
	}
	err = pm.progress.Remove(taskID)
	if err != nil && !cdnerrors.IsDataNotFound(err) {
		return errors.Wrap(err, "failed to clear progress record")
	}
	return nil
}

func (pm *Manager) GetPieces(ctx context.Context, taskID string) (records []*types.SeedPiece, err error) {
	//pm.mu.GetLock(taskID, true)
	//defer pm.mu.ReleaseLock(taskID, true)
	return pm.getPieceMetaRecordsByTaskID(taskID)
}
