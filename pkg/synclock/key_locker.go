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
)

var defaultLocker = NewKeyLocker()

type KeyLocker struct {
	// use syncPool to cache allocated but unused *countRWMutex items for later reuse
	syncPool *sync.Pool

	lockerMap map[string]*countRWMutex

	mutex sync.Mutex
}

func NewKeyLocker() *KeyLocker {
	return &KeyLocker{
		syncPool: &sync.Pool{
			New: func() interface{} {
				return newCountRWMutex()
			},
		},
		lockerMap: make(map[string]*countRWMutex),
	}
}

func Lock(key string, rLock bool) {
	defaultLocker.Lock(key, rLock)
}

func UnLock(key string, rLock bool) {
	defaultLocker.UnLock(key, rLock)
}

func (l *KeyLocker) Lock(key string, rLock bool) {
	l.mutex.Lock()
	locker, ok := l.lockerMap[key]
	if !ok {
		locker = l.syncPool.Get().(*countRWMutex)
		l.lockerMap[key] = locker
	}
	locker.inc()
	l.mutex.Unlock()

	locker.lock(rLock)
}

func (l *KeyLocker) UnLock(key string, rLock bool) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	locker, ok := l.lockerMap[key]
	if !ok {
		return
	}

	locker.unlock(rLock)

	if locker.dec() <= 0 {
		locker.reset()
		l.syncPool.Put(locker)
		delete(l.lockerMap, key)
	}
}
