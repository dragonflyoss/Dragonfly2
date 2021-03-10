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

package synclock

import (
	"sync"
	"sync/atomic"
)

type countRWMutex struct {
	sync.RWMutex
	c int32
}

func newCountRWMutex() *countRWMutex {
	return &countRWMutex{}
}

func (cm *countRWMutex) reset() {
	atomic.StoreInt32(&cm.c, 0)
}

func (cm *countRWMutex) inc() int32 {
	return atomic.AddInt32(&cm.c, 1)
}

func (cm *countRWMutex) dec() int32 {
	return atomic.AddInt32(&cm.c, -1)
}

func (cm *countRWMutex) lock(rLock bool) {
	if rLock {
		cm.RLock()
	} else {
		cm.Lock()
	}
}

func (cm *countRWMutex) unlock(rLock bool) {
	if rLock {
		cm.RUnlock()
	} else {
		cm.Unlock()
	}
}
