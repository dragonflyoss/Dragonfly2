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

import "sync"

type Item interface {
	SortedValue() int
}

type SortedMap interface {
	Add(string, Item)
}

type sortedMap struct {
	mu     *sync.RWMutex
	bucket []string
	data   *sync.Map
}

func New(len int) SortedMap {
	return &sortedMap{
		mu:     &sync.RWMutex{},
		bucket: make([]string, len),
		data:   &sync.Map{},
	}
}

func (s *sortedMap) Add(key string, item Item) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.bucket[item.SortedValue()] = key
	s.data.Store(key, item)
}
