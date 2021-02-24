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

package lockerutils

import (
	"go.uber.org/atomic"
	"sync"
)

type countRWMutex struct {
	count *atomic.Int32
	sync.RWMutex
}

func newCountRWMutex() *countRWMutex {
	return &countRWMutex{
		count: atomic.NewInt32(0),
	}
}

func (cr *countRWMutex) reset() {
	cr.count.Store(0)
}

func (cr *countRWMutex) increaseCount() int32 {
	return cr.count.Inc()
}

func (cr *countRWMutex) decreaseCount() int32 {
	return cr.count.Dec()
}

func (cr *countRWMutex) lock(ro bool) {
	if ro {
		cr.RLock()
		return
	}
	cr.Lock()
}

func (cr *countRWMutex) unlock(ro bool) {
	if ro {
		cr.RUnlock()
		return
	}
	cr.Unlock()
}
