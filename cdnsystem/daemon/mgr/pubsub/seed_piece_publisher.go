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

package pubsub

import (
	"container/list"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/util"
	"github.com/dragonflyoss/Dragonfly2/pkg/dferrors"
	"github.com/dragonflyoss/Dragonfly2/pkg/struct/syncmap"
	"github.com/pkg/errors"
	"sync"
	"time"
)

type SeedSubscriber chan types.SeedPiece

type SeedPiecePublisher struct {
	seedSubscribers      *syncmap.SyncMap
	buffer               int
	timeout              time.Duration
	taskPieceMetaRecords *syncmap.SyncMap
	m                    *util.LockerPool
}

func NewPublisher(publishTimeout time.Duration, buffer int) *SeedPiecePublisher {
	return &SeedPiecePublisher{
		seedSubscribers:      syncmap.NewSyncMap(),
		buffer:               buffer,
		timeout:              publishTimeout,
		taskPieceMetaRecords: syncmap.NewSyncMap(),
		m:                    util.NewLockerPool(),
	}
}

// SubscribeTask subscribe task's piece seed
func (p *SeedPiecePublisher) SubscribeTask(taskID string) (SeedSubscriber, error) {
	p.m.GetLock(taskID, false)
	defer p.m.ReleaseLock(taskID, false)
	chanList, err := p.seedSubscribers.GetAsList(taskID)
	if err != nil && !dferrors.IsDataNotFound(err) {
		return nil, errors.Wrap(err, "failed to get seed subscribers")
	}
	if dferrors.IsDataNotFound(err) {
		chanList = list.New()
		p.seedSubscribers.Add(taskID, chanList)
	}
	ch := make(SeedSubscriber, p.buffer)
	chanList.PushBack(ch)
	pieceMetaDataRecords, err := p.getPieceMetaRecordsByTaskID(taskID)
	if err != nil && !dferrors.IsDataNotFound(err) {
		return nil, errors.Wrap(err, "failed to get piece meta records by taskId")
	}
	for _, pieceMetaRecord := range pieceMetaDataRecords {
		ch <- pieceMetaRecord
	}
	return ch, nil
}

//UnSubscribeTask unsubscribe task's piece seed
func (p *SeedPiecePublisher) UnSubscribeTask(sub SeedSubscriber, taskID string) error {
	p.m.GetLock(taskID, false)
	defer p.m.ReleaseLock(taskID, false)
	chanList, err := p.seedSubscribers.GetAsList(taskID)
	if err != nil && !dferrors.IsDataNotFound(err) {
		return errors.Wrap(err, "failed to get seed subscribers")
	}
	for e := chanList.Front(); e != nil; e = e.Next() {
		if e.Value.(SeedSubscriber) == sub {
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
	p.setPieceMetaRecord(taskID, record)
	var wg sync.WaitGroup
	chanList, err := p.seedSubscribers.GetAsList(taskID)
	if err != nil && !dferrors.IsDataNotFound(err) {
		return errors.Wrap(err, "failed to get seed subscribers")
	}
	for e := chanList.Front(); e != nil; e = e.Next() {
		wg.Add(1)
		sub := e.Value.(SeedSubscriber)
		go func(sub SeedSubscriber, wg *sync.WaitGroup) {
			defer wg.Done()
			select {
			case sub <- record:
				return
			case <-time.After(p.timeout):
			}
		}(sub, &wg)
	}
	wg.Wait()
	return nil
}

// setPieceMetaRecord
func (p *SeedPiecePublisher) setPieceMetaRecord(taskID string, record types.SeedPiece) error {
	pieceRecords, err := p.taskPieceMetaRecords.GetAsList(taskID)
	if err != nil && !dferrors.IsDataNotFound(err) {
		return err
	}
	if pieceRecords == nil {
		pieceRecords := list.New()
		p.taskPieceMetaRecords.Add(taskID, pieceRecords)
	}
	pieceRecords.PushBack(record)
	return nil
}

// getPieceMetaRecordsByTaskID
func (p *SeedPiecePublisher) getPieceMetaRecordsByTaskID(taskID string) (records []types.SeedPiece, err error) {
	pieceRecords, err := p.taskPieceMetaRecords.GetAsList(taskID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get piece meta records")
	}
	for e := pieceRecords.Front(); e != nil; e = e.Next() {

		records = append(records, e.Value.(types.SeedPiece))
	}
	return records, nil
}

func (p *SeedPiecePublisher) Close(taskID string) error {
	p.m.GetLock(taskID, false)
	defer p.m.ReleaseLock(taskID, false)
	chanList, err := p.seedSubscribers.GetAsList(taskID)
	if err != nil && !dferrors.IsDataNotFound(err) {
		return errors.Wrap(err, "failed to get seed subscribers")
	}
	var wg sync.WaitGroup
	for e := chanList.Front(); e != nil; e = e.Next() {
		wg.Add(1)
		sub := e.Value.(SeedSubscriber)
		go func(sub SeedSubscriber, wg *sync.WaitGroup) {
			defer wg.Done()
			close(sub)
		}(sub, &wg)
	}
	wg.Wait()
	return nil
}
