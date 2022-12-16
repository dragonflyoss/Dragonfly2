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

package ring

import (
	"math"
	"sync"
)

type sequence[T any] struct {
	locker  sync.Locker
	enqCond *sync.Cond
	deqCond *sync.Cond
	closed  bool

	queue      []*T
	head, tail uint64
	cap, mask  uint64
}

func NewSequence[T any](exponent int) Queue[T] {
	return newSequence[T](exponent)
}

func newSequence[T any](exponent int) *sequence[T] {
	capacity := uint64(math.Exp2(float64(exponent)))
	locker := &sync.Mutex{}
	return &sequence[T]{
		locker: locker,
		enqCond: &sync.Cond{
			L: locker,
		},
		deqCond: &sync.Cond{
			L: locker,
		},
		queue: make([]*T, capacity),
		head:  uint64(0),
		tail:  uint64(0),
		cap:   capacity,
		mask:  capacity - 1,
	}
}

func (seq *sequence[T]) Enqueue(value *T) {
	seq.locker.Lock()

entry:
	if seq.closed {
		seq.locker.Unlock()
		return
	}

	if seq.isFull() {
		seq.enqCond.Wait()
		goto entry
	}

	newTail := (seq.tail + 1) & seq.mask
	seq.tail = newTail
	seq.queue[newTail] = value

	seq.deqCond.Signal()
	seq.locker.Unlock()
}

func (seq *sequence[T]) Dequeue() (value *T, ok bool) {
	seq.locker.Lock()
entry:
	if seq.closed {
		seq.locker.Unlock()
		return nil, false
	}

	if seq.isEmpty() {
		seq.deqCond.Wait()
		goto entry
	}
	newHead := (seq.head + 1) & seq.mask
	seq.head = newHead
	val := seq.queue[newHead]

	seq.enqCond.Signal()
	seq.locker.Unlock()
	return val, true
}

func (seq *sequence[T]) Close() {
	seq.locker.Lock()
	seq.closed = true
	seq.enqCond.Broadcast()
	seq.deqCond.Broadcast()
	seq.locker.Unlock()
}

func (seq *sequence[T]) isEmpty() bool {
	return seq.head == seq.tail
}

func (seq *sequence[T]) isFull() bool {
	return seq.tail-seq.head == seq.cap-1 || seq.head-seq.tail == 1
}
