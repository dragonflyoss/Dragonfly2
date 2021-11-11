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

package sortedmap

import (
	"sync"

	"d7y.io/dragonfly/v2/pkg/set"
)

type Bucket interface {
	Len() uint
	Add(uint, interface{}) bool
	Delete(uint, interface{})
	Contains(uint, interface{}) bool
	Range(func(interface{}) bool)
	ReverseRange(fn func(interface{}) bool)
}

type bucket struct {
	mu   *sync.RWMutex
	data []set.Set
}

func NewBucket(len uint) Bucket {
	return &bucket{
		mu:   &sync.RWMutex{},
		data: make([]set.Set, len),
	}
}

func (b *bucket) Add(i uint, v interface{}) bool {
	if b.Len() <= i {
		return false
	}

	b.mu.Lock()
	if s := b.data[i]; s == nil {
		b.data[i] = set.New()
	}
	b.mu.Unlock()

	if ok := b.data[i].Add(v); !ok {
		return false
	}

	return true
}

func (b *bucket) Delete(i uint, v interface{}) {
	if b.Len() <= i {
		return
	}

	b.mu.RLock()
	if s := b.data[i]; s == nil {
		b.mu.RUnlock()
		return
	}
	b.mu.RUnlock()

	b.data[i].Delete(v)
}

func (b *bucket) Contains(i uint, v interface{}) bool {
	if b.Len() <= i {
		return false
	}

	b.mu.RLock()
	if s := b.data[i]; s == nil {
		b.mu.RUnlock()
		return false
	}
	b.mu.RUnlock()

	if ok := b.data[i].Contains(v); !ok {
		return false
	}

	return true
}

func (b *bucket) Len() uint {
	return uint(len(b.data))
}

func (b *bucket) Range(fn func(interface{}) bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, s := range b.data {
		if s != nil && s.Len() > 0 {
			for _, v := range s.Values() {
				if !fn(v) {
					return
				}
			}
		}
	}
}

func (b *bucket) ReverseRange(fn func(interface{}) bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for i := range b.data {
		s := b.data[b.Len()-uint(1+i)]
		if s != nil && s.Len() > 0 {
			for _, v := range s.Values() {
				if !fn(v) {
					return
				}
			}
		}
	}
}
