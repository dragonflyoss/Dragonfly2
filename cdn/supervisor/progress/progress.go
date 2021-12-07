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
	"sync"

	"go.uber.org/atomic"

	"d7y.io/dragonfly/v2/cdn/supervisor/task"
	logger "d7y.io/dragonfly/v2/internal/dflog"
)

type subscriber struct {
	ctx       context.Context
	scheduler string
	taskID    string
	done      chan struct{}
	once      sync.Once
	pieces    map[uint32]*task.PieceInfo
	pieceChan chan *task.PieceInfo
	cond      *sync.Cond
	closed    *atomic.Bool
}

func newProgressSubscriber(ctx context.Context, clientAddr, taskID string, taskPieces map[uint32]*task.PieceInfo) *subscriber {
	pieces := make(map[uint32]*task.PieceInfo, len(taskPieces))
	for u, info := range taskPieces {
		pieces[u] = info
	}
	sub := &subscriber{
		ctx:       ctx,
		scheduler: clientAddr,
		taskID:    taskID,
		pieces:    pieces,
		done:      make(chan struct{}),
		pieceChan: make(chan *task.PieceInfo, 100),
		cond:      sync.NewCond(&sync.Mutex{}),
		closed:    atomic.NewBool(false),
	}
	go sub.readLoop()
	return sub
}

func (sub *subscriber) readLoop() {
	logger.Debugf("subscriber %s starts watching task %s seed progress", sub.scheduler, sub.taskID)
	defer func() {
		close(sub.pieceChan)
		logger.Debugf("subscriber %s stopped watch task %s seed progress", sub.scheduler, sub.taskID)
	}()
	for {
		select {
		case <-sub.ctx.Done():
			return
		case <-sub.done:
			if len(sub.pieces) == 0 {
				return
			}
		default:
			sub.cond.L.Lock()
			for len(sub.pieces) == 0 && !sub.closed.Load() {
				sub.cond.Wait()
			}
			for i, info := range sub.pieces {
				sub.pieceChan <- info
				delete(sub.pieces, i)
			}
			sub.cond.L.Unlock()
		}
	}
}

func (sub *subscriber) Notify(seedPiece *task.PieceInfo) {
	logger.Debugf("notifies subscriber %s about %d piece info of taskID %s", sub.scheduler, seedPiece.PieceNum, sub.taskID)
	sub.cond.L.Lock()
	sub.pieces[seedPiece.PieceNum] = seedPiece
	sub.cond.L.Unlock()
	sub.cond.Signal()
}

func (sub *subscriber) Receiver() <-chan *task.PieceInfo {
	return sub.pieceChan
}

func (sub *subscriber) Close() {
	sub.once.Do(func() {
		logger.Debugf("close subscriber %s from taskID %s", sub.scheduler, sub.taskID)
		sub.cond.Signal()
		sub.closed.CAS(false, true)
		close(sub.done)
	})
}

type publisher struct {
	taskID      string
	subscribers *list.List
}

func newProgressPublisher(taskID string) *publisher {
	return &publisher{
		taskID:      taskID,
		subscribers: list.New(),
	}
}

func (pub *publisher) AddSubscriber(sub *subscriber) {
	pub.subscribers.PushBack(sub)
	logger.Debugf("subscriber %s has been added into subscribers of publisher %s, list size is %d", sub.scheduler, sub.taskID, pub.subscribers.Len())
}

func (pub *publisher) RemoveSubscriber(sub *subscriber) {
	sub.Close()
	for e := pub.subscribers.Front(); e != nil; e = e.Next() {
		if e.Value == sub {
			pub.subscribers.Remove(e)
			logger.Debugf("subscriber %s has been removed from subscribers of publisher %s, list size is %d", sub.scheduler, sub.taskID, pub.subscribers.Len())
			return
		}
	}
}

func (pub *publisher) NotifySubscribers(seedPiece *task.PieceInfo) {
	for e := pub.subscribers.Front(); e != nil; e = e.Next() {
		e.Value.(*subscriber).Notify(seedPiece)
	}
}

func (pub *publisher) RemoveAllSubscribers() {
	var next *list.Element
	for e := pub.subscribers.Front(); e != nil; e = next {
		next = e.Next()
		pub.RemoveSubscriber(e.Value.(*subscriber))
	}
}
