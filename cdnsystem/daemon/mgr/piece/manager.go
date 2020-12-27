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
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/daemon/mgr"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/daemon/mgr/cdn"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/util"
	"github.com/dragonflyoss/Dragonfly2/pkg/dferrors"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
	"github.com/dragonflyoss/Dragonfly2/pkg/struct/syncmap"
	"github.com/pkg/errors"
	"sync"
	"time"
)

type SeedPiecePublisher struct {
	seedSubscribers  *syncmap.SyncMap
	buffer           int
	timeout          time.Duration
	m                util.LockerPool
}

func NewPublisher(publishTimeout time.Duration, buffer int) *SeedPiecePublisher {
	return &SeedPiecePublisher{
		buffer:          buffer,
		timeout:         publishTimeout,
		seedSubscribers: syncmap.NewSyncMap(),
	}
}

func (p *SeedPiecePublisher) Create(taskID string) error {
	return nil
}

// SubscribeTask subscribe task's piece seed
func (p *SeedPiecePublisher) SubscribeTask(taskID string) (mgr.SeedSubscriber, error) {
	p.m.GetLock(taskID, false)
	defer p.m.ReleaseLock(taskID, false)
	chanList, err := p.seedSubscribers.GetAsList(taskID)
	if err != nil && !dferrors.IsDataNotFound(err) {
		return nil, errors.Wrapf(err, "taskID:%s, get seed subscribers fail", taskID)
	}
	if dferrors.IsDataNotFound(err) {
		chanList = list.New()
		p.seedSubscribers.Add(taskID, chanList)
	}
	ch := make(mgr.SeedSubscriber, p.buffer)
	chanList.PushBack(ch)
	pieceMetaDataRecords, err := p.pieceMetaDataMgr.getPieceMetaRecordsByTaskID(taskID)
	if err != nil && !dferrors.IsDataNotFound(err) {
		return nil, errors.Wrapf(err, "taskID:%s, get piece meta records by taskId fail", taskID)
	}
	for _, pieceMetaRecord := range pieceMetaDataRecords {
		ch <- pieceMetaRecord
	}
	return ch, nil
}

//UnSubscribeTask unsubscribe task's piece seed
func (p *SeedPiecePublisher) UnSubscribeTask(sub mgr.SeedSubscriber, taskID string) error {
	p.m.GetLock(taskID, false)
	defer p.m.ReleaseLock(taskID, false)
	chanList, err := p.seedSubscribers.GetAsList(taskID)
	if err != nil && !dferrors.IsDataNotFound(err) {
		return errors.Wrapf(err, "taskID:%s, get seed subscribers fail", taskID)
	}
	for e := chanList.Front(); e != nil; e = e.Next() {
		if e.Value.(mgr.SeedSubscriber) == sub {
			chanList.Remove(e)
			break
		}
	}
	close(sub)
	return nil
}

func (p *SeedPiecePublisher) Publish(taskID string, record types.SeedPiece) error {
	p.m.GetLock(taskID, false)
	defer p.m.ReleaseLock(taskID, false)
	err := p.pieceMetaDataMgr.setPieceMetaRecord(taskID, record)
	if err != nil {
		logger.Errorf("taskID: %s, set piece meta record fail, pieceRecord:%v", taskID, record)
	}
	var wg sync.WaitGroup
	chanList, err := p.seedSubscribers.GetAsList(taskID)
	if err != nil && !dferrors.IsDataNotFound(err) {
		return errors.Wrapf(err, "taskID:%s, get seed subscribers fail", taskID)
	}
	for e := chanList.Front(); e != nil; e = e.Next() {
		wg.Add(1)
		sub := e.Value.(mgr.SeedSubscriber)
		go func(sub mgr.SeedSubscriber, wg *sync.WaitGroup) {
			defer wg.Done()
			select {
			case sub <- record:
			case <-time.After(p.timeout):
			}
		}(sub, &wg)
	}
	wg.Wait()
	return nil
}

func (p *SeedPiecePublisher) Close(taskID string) error {
	p.m.GetLock(taskID, false)
	defer p.m.ReleaseLock(taskID, false)
	chanList, err := p.seedSubscribers.GetAsList(taskID)
	if err != nil && !dferrors.IsDataNotFound(err) {
		return errors.Wrapf(err, "taskID:%s, get seed subscribers fail", taskID)
	}
	var wg sync.WaitGroup
	for e := chanList.Front(); e != nil; e = e.Next() {
		wg.Add(1)
		sub := e.Value.(mgr.SeedSubscriber)
		go func(sub mgr.SeedSubscriber, wg *sync.WaitGroup) {
			defer wg.Done()
			close(sub)
		}(sub, &wg)
	}
	wg.Wait()
	return nil
}

func (p *SeedPiecePublisher) GetPieceMetaRecordsByTaskID(taskId string) (pieceMetaRecords []types.SeedPiece, error) {
	return p.pieceMetaDataMgr.getPieceMetaRecordsByTaskID(taskId)
}
