/*
 *     Copyright 2022 The Dragonfly Authors
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

package peer

type pieceBroker struct {
	stopCh    chan struct{}
	publishCh chan *PieceInfo
	subCh     chan chan *PieceInfo
	unsubCh   chan chan *PieceInfo
}

type PieceInfo struct {
	// Num is the current piece num
	Num int32
	// OrderedNum is the max pieces num with ordered, eg: 0 1 2 3 5 7 8, the OrderedNum is 3
	OrderedNum int32
	Finished   bool
}

func newPieceBroker() *pieceBroker {
	return &pieceBroker{
		stopCh:    make(chan struct{}),
		publishCh: make(chan *PieceInfo, 10),
		subCh:     make(chan chan *PieceInfo),
		unsubCh:   make(chan chan *PieceInfo),
	}
}

func (b *pieceBroker) Start() {
	var (
		orderedNum int32 = -1
		subs             = map[chan *PieceInfo]struct{}{}
		pieces           = map[int32]struct{}{}
	)

	for {
		select {
		case <-b.stopCh:
			//for msgCh := range subs {
			//	close(msgCh)
			//}
			return
		case msgCh := <-b.subCh:
			subs[msgCh] = struct{}{}
		case msgCh := <-b.unsubCh:
			delete(subs, msgCh)
		case msg := <-b.publishCh:
			pieces[msg.Num] = struct{}{}
			if orderedNum+1 == msg.Num {
				orderedNum++
				// search cached pieces
				for {
					if _, ok := pieces[orderedNum+1]; ok {
						orderedNum++
					} else {
						break
					}
				}
			}
			msg.OrderedNum = orderedNum
			for msgCh := range subs {
				// msgCh is buffered, use non-blocking send to protect the broker
				select {
				case msgCh <- msg:
				default:
				}
			}
		}
	}
}

func (b *pieceBroker) Stop() {
	close(b.stopCh)
}

func (b *pieceBroker) Subscribe() chan *PieceInfo {
	msgCh := make(chan *PieceInfo, 5)
	select {
	case <-b.stopCh:
	case b.subCh <- msgCh:
	}

	return msgCh
}

func (b *pieceBroker) Unsubscribe(msgCh chan *PieceInfo) {
	b.unsubCh <- msgCh
}

func (b *pieceBroker) Publish(msg *PieceInfo) {
	select {
	case b.publishCh <- msg:
	case <-b.stopCh:
	}
}
