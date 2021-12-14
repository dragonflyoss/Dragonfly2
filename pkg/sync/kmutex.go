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

type Kmutex struct {
	m *sync.Map
}

func NewKmutex() *Kmutex {
	m := sync.Map{}
	return &Kmutex{&m}
}

func (k *Kmutex) Lock(key interface{}) {
	rm, _ := k.m.LoadOrStore(key, &sync.Mutex{})
	mu := rm.(*sync.Mutex)
	mu.Lock()
}

func (k *Kmutex) Unlock(key interface{}) {
	rm, ok := k.m.Load(key)
	if !ok {
		return
	}

	mu := rm.(*sync.Mutex)
	k.m.Delete(key)
	mu.Unlock()
}
