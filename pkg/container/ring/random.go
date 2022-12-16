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
	"math/rand"
	"time"
)

type random[T any] struct {
	*sequence[T]
	seed *rand.Rand
}

func NewRandom[T any](exponent int) Queue[T] {
	return &random[T]{
		sequence: newSequence[T](exponent),
		seed:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
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
