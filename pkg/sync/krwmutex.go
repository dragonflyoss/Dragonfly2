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

package sync

import (
	"sync"
)

type Krwmutex struct {
	m *sync.Map
}

func NewKrwmutex() *Krwmutex {
	m := sync.Map{}
	return &Krwmutex{&m}
}

func (k *Krwmutex) Lock(key interface{}) {
	rm, _ := k.m.LoadOrStore(key, &sync.RWMutex{})
	mu := rm.(*sync.RWMutex)
	mu.Lock()
}

func (k *Krwmutex) Unlock(key interface{}) {
	rm, ok := k.m.Load(key)
	if !ok {
		return
	}

	mu := rm.(*sync.RWMutex)
	k.m.Delete(key)
	mu.Unlock()
}

func (k *Krwmutex) RLock(key interface{}) {
	rm, _ := k.m.LoadOrStore(key, &sync.RWMutex{})
	mu := rm.(*sync.RWMutex)
	mu.RLock()
}

func (k *Krwmutex) RUnlock(key interface{}) {
	rm, ok := k.m.Load(key)
	if !ok {
		return
	}

	mu := rm.(*sync.RWMutex)
	k.m.Delete(key)
	mu.RUnlock()
}
