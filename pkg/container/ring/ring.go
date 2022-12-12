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
	"math/rand"
	"sync"
	"time"
)

type Queue[T any] interface {
	Enqueue(value *T)
	Dequeue() (value *T, ok bool)
	Close()
}

type sequence[T any] struct {
	locker  sync.Locker
	enqCond *sync.Cond
	deqCond *sync.Cond
	closed  bool

	queue      []*T
	head, tail uint64
	cap, mask  uint64
}

type random[T any] struct {
	*sequence[T]
	seed *rand.Rand
}

func NewSequence[T any](exponent int) Queue[T] {
	return newSequence[T](exponent)
}

func NewRandom[T any](exponent int) Queue[T] {
	return &random[T]{
		sequence: newSequence[T](exponent),
		seed:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
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

func (r *random[T]) Enqueue(value *T) {
	r.sequence.Enqueue(value)
}

func (r *random[T]) Dequeue() (value *T, ok bool) {
	r.locker.Lock()
entry:
	if r.closed {
		r.locker.Unlock()
		return nil, false
	}

	if r.isEmpty() {
		r.deqCond.Wait()
		goto entry
	}

	var (
		count   uint64
		newHead = (r.head + 1) & r.mask
	)

	if r.head < r.tail {
		count = r.tail - r.head
	} else {
		count = r.mask - (r.head - r.tail) + 1
	}

	if count > 1 {
		// generate a random index
		idx := r.seed.Int63n(int64(count))
		randomHead := (newHead + uint64(idx)) & r.mask
		// skip same idx with newHeader
		if idx > 0 {
			// swap the new idx and newHead
			var tmp *T
			tmp = r.queue[randomHead]
			r.queue[randomHead] = r.queue[newHead]
			r.queue[newHead] = tmp
		}
	}

	r.head = newHead
	val := r.queue[newHead]

	r.enqCond.Signal()
	r.locker.Unlock()
	return val, true
}

func (r *random[T]) Close() {
	r.sequence.Close()
}
