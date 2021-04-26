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
	"d7y.io/dragonfly/v2/cdnsystem/config"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/mgr"
	"d7y.io/dragonfly/v2/cdnsystem/types"
	"d7y.io/dragonfly/v2/pkg/dferrors"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/structure/syncmap"
	"d7y.io/dragonfly/v2/pkg/synclock"
	"github.com/pkg/errors"
	"sync"
	"time"
)

func init() {
	// Ensure that Manager implements the SeedProgressMgr interface
	var manager *Manager = nil
	var _ mgr.SeedProgressMgr = manager
}

type Manager struct {
	cfg                  *config.Config
	seedSubscribers      *syncmap.SyncMap
	taskPieceMetaRecords *syncmap.SyncMap
	taskMgr              mgr.SeedTaskMgr
	mu                   *synclock.LockerPool
	timeout              time.Duration
	buffer               int
}

func (pm *Manager) SetTaskMgr(taskMgr mgr.SeedTaskMgr) {
	pm.taskMgr = taskMgr
}

func NewManager(cfg *config.Config) (*Manager, error) {
	return &Manager{
		cfg:                  cfg,
		seedSubscribers:      syncmap.NewSyncMap(),
		taskPieceMetaRecords: syncmap.NewSyncMap(),
		mu:                   synclock.NewLockerPool(),
		timeout:              3 * time.Second,
		buffer:               4,
	}, nil
}

func (pm *Manager) InitSeedProgress(ctx context.Context, taskId string) {
	pm.mu.Lock(taskId, true)
	if _, ok := pm.seedSubscribers.Load(taskId); ok {
		logger.WithTaskID(taskId).Debugf("the task seedSubscribers already exist")
		if _, ok := pm.taskPieceMetaRecords.Load(taskId); ok {
			logger.WithTaskID(taskId).Debugf("the task taskPieceMetaRecords already exist")
			pm.mu.UnLock(taskId, true)
			return
		}
	}
	pm.mu.UnLock(taskId, true)
	pm.mu.Lock(taskId, false)
	defer pm.mu.UnLock(taskId, false)
	if _, loaded := pm.seedSubscribers.LoadOrStore(taskId, list.New()); loaded {
		logger.WithTaskID(taskId).Info("the task seedSubscribers already exist")
	}
	if _, loaded := pm.taskPieceMetaRecords.LoadOrStore(taskId, syncmap.NewSyncMap()); loaded {
		logger.WithTaskID(taskId).Info("the task taskPieceMetaRecords already exist")
	}
}

func (pm *Manager) WatchSeedProgress(ctx context.Context, taskId string) (<-chan *types.SeedPiece, error) {
	logger.Debugf("watch seed progress begin for taskId:%s", taskId)
	pm.mu.Lock(taskId, true)
	defer pm.mu.UnLock(taskId, true)
	chanList, err := pm.seedSubscribers.GetAsList(taskId)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get seed subscribers")
	}
	pieceMetaDataRecords, err := pm.getPieceMetaRecordsByTaskId(taskId)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get piece meta records by taskId")
	}
	ch := make(chan *types.SeedPiece, pm.buffer)
	ele := chanList.PushBack(ch)
	go func(seedCh chan *types.SeedPiece, ele *list.Element) {
		for _, pieceMetaRecord := range pieceMetaDataRecords {
			logger.Debugf("seed piece meta record %+v", pieceMetaRecord)
			select {
			case seedCh <- pieceMetaRecord:
			case <-time.After(pm.timeout):
			}
		}
		if task, err := pm.taskMgr.Get(ctx, taskId); err == nil && task.IsDone() {
			chanList.Remove(ele)
			close(seedCh)
		}
	}(ch, ele)
	return ch, nil
}

// Publish publish seedPiece
func (pm *Manager) PublishPiece(ctx context.Context, taskId string, record *types.SeedPiece) error {
	logger.Debugf("seed piece meta record %+v", record)
	pm.mu.Lock(taskId, false)
	defer pm.mu.UnLock(taskId, false)
	err := pm.setPieceMetaRecord(taskId, record)
	if err != nil {
		errors.Wrap(err, "failed to set piece meta record")
	}
	chanList, err := pm.seedSubscribers.GetAsList(taskId)
	if err != nil {
		return errors.Wrap(err, "failed to get seed subscribers")
	}
	var wg sync.WaitGroup
	for e := chanList.Front(); e != nil; e = e.Next() {
		wg.Add(1)
		sub := e.Value.(chan *types.SeedPiece)
		go func(sub chan *types.SeedPiece, record *types.SeedPiece) {
			defer wg.Done()
			select {
			case sub <- record:
			case <-time.After(pm.timeout):
			}

		}(sub, record)
	}
	wg.Wait()
	return nil
}

func (pm *Manager) PublishTask(ctx context.Context, taskId string, task *types.SeedTask) error {
	logger.Debugf("publish task record %+v", task)
	pm.mu.Lock(taskId, false)
	defer pm.mu.UnLock(taskId, false)
	chanList, err := pm.seedSubscribers.GetAsList(taskId)
	if err != nil {
		return errors.Wrap(err, "failed to get seed subscribers")
	}
	// unwatch
	for e := chanList.Front(); e != nil; e = e.Next() {
		chanList.Remove(e)
		sub, ok := e.Value.(chan *types.SeedPiece)
		if !ok {
			logger.Warnf("failed to convert chan seedPiece, e.Value:%v", e.Value)
			continue
		}
		close(sub)
	}
	return nil
}

func (pm *Manager) Clear(ctx context.Context, taskID string) error {
	pm.mu.Lock(taskID, false)
	defer pm.mu.UnLock(taskID, false)
	chanList, err := pm.seedSubscribers.GetAsList(taskID)
	if err != nil && errors.Cause(err) != dferrors.ErrDataNotFound {
		return errors.Wrap(err, "failed to get seed subscribers")
	}
	if chanList != nil {
		for e := chanList.Front(); e != nil; e = e.Next() {
			chanList.Remove(e)
			sub, ok := e.Value.(chan *types.SeedPiece)
			if !ok {
				logger.Warnf("failed to convert chan seedPiece, e.Value:%v", e.Value)
				continue
			}
			close(sub)
		}
		chanList = nil
	}
	err = pm.seedSubscribers.Remove(taskID)
	if err != nil && dferrors.ErrDataNotFound != errors.Cause(err) {
		return errors.Wrap(err, "failed to clear seed subscribes")
	}
	err = pm.taskPieceMetaRecords.Remove(taskID)
	if err != nil && dferrors.ErrDataNotFound != errors.Cause(err) {
		return errors.Wrap(err, "failed to clear piece meta records")
	}
	return nil
}

func (pm *Manager) GetPieces(ctx context.Context, taskID string) (records []*types.SeedPiece, err error) {
	pm.mu.Lock(taskID, true)
	defer pm.mu.UnLock(taskID, true)
	return pm.getPieceMetaRecordsByTaskId(taskID)
}
