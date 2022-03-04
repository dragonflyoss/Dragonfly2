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
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/cdn/supervisor/task"
	"d7y.io/dragonfly/v2/pkg/util/rangeutils"
)

func Test_publisher_NotifySubscribers(t *testing.T) {
	assert := assert.New(t)
	publisher := newProgressPublisher("testTask")
	notifyPieces := []*task.PieceInfo{
		{
			PieceNum: 0,
			PieceMd5: "pieceMd51",
			PieceRange: &rangeutils.Range{
				StartIndex: 0,
				EndIndex:   99,
			},
			OriginRange: &rangeutils.Range{
				StartIndex: 0,
				EndIndex:   99,
			},
			PieceLen:   100,
			PieceStyle: 0,
		}, {
			PieceNum: 1,
			PieceMd5: "pieceMd52",
			PieceRange: &rangeutils.Range{
				StartIndex: 100,
				EndIndex:   199,
			},
			OriginRange: &rangeutils.Range{
				StartIndex: 100,
				EndIndex:   199,
			},
			PieceLen:   100,
			PieceStyle: 0,
		},
	}
	wg := sync.WaitGroup{}
	sub1 := newProgressSubscriber(context.Background(), "client1", "testTask", nil)
	publisher.AddSubscriber(sub1)

	sub2 := newProgressSubscriber(context.Background(), "client2", "testTask", nil)
	publisher.AddSubscriber(sub2)
	additionPieceInfo1 := &task.PieceInfo{
		PieceNum:    100,
		PieceMd5:    "xxxxx",
		PieceRange:  &rangeutils.Range{},
		OriginRange: &rangeutils.Range{},
		PieceLen:    0,
		PieceStyle:  0,
	}
	sub3 := newProgressSubscriber(context.Background(), "client3", "taskTask", []*task.PieceInfo{
		additionPieceInfo1,
	})
	additionPieceInfo2 := &task.PieceInfo{
		PieceNum:    200,
		PieceMd5:    "xxxxx",
		PieceRange:  &rangeutils.Range{},
		OriginRange: &rangeutils.Range{},
		PieceLen:    0,
		PieceStyle:  0,
	}
	publisher.AddSubscriber(sub3)
	sub4 := newProgressSubscriber(context.Background(), "client4", "taskTask", []*task.PieceInfo{
		additionPieceInfo1,
		additionPieceInfo2,
	})
	publisher.AddSubscriber(sub4)
	chan1 := sub1.Receiver()
	chan2 := sub2.Receiver()
	chan3 := sub3.Receiver()
	chan4 := sub4.Receiver()
	wg.Add(1)
	go func(pieceChan <-chan *task.PieceInfo) {
		defer wg.Done()
		var pieceCount = 0
		for info := range pieceChan {
			pieceCount++
			assert.EqualValues(notifyPieces[info.PieceNum], info)
		}
		assert.Equal(2, pieceCount)
	}(chan1)
	wg.Add(1)
	go func(pieceChan <-chan *task.PieceInfo) {
		defer wg.Done()
		var pieceCount = 0
		for info := range pieceChan {
			pieceCount++
			assert.EqualValues(notifyPieces[info.PieceNum], info)
		}
		assert.Equal(2, pieceCount)
	}(chan2)
	wg.Add(1)
	go func(pieceChan <-chan *task.PieceInfo) {
		defer wg.Done()
		var pieceCount = 0
		for info := range pieceChan {
			pieceCount++
			if info.PieceNum == 100 {
				assert.EqualValues(additionPieceInfo1, info)
			} else {
				assert.EqualValues(notifyPieces[info.PieceNum], info)
			}
		}
	}(chan3)
	wg.Add(1)
	go func(pieceChan <-chan *task.PieceInfo) {
		defer wg.Done()
		var pieceCount = 0
		for info := range pieceChan {
			pieceCount++
			if info.PieceNum == 100 {
				assert.EqualValues(additionPieceInfo1, info)
			} else if info.PieceNum == 200 {
				assert.EqualValues(additionPieceInfo2, info)
			} else {
				assert.EqualValues(notifyPieces[info.PieceNum], info)
			}
		}
		assert.Equal(4, pieceCount)
	}(chan4)

	// notify all subscribers
	for i := range notifyPieces {
		publisher.NotifySubscribers(notifyPieces[i])
	}
	publisher.RemoveAllSubscribers()
	wg.Wait()
}
