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

package set

import (
	"sync"
)

type Set interface {
	Values() []interface{}
	Add(interface{}) bool
	Delete(interface{})
	Contains(...interface{}) bool
	Len() uint
	Range(func(interface{}) bool)
}

type set struct {
	mu   *sync.RWMutex
	data map[interface{}]struct{}
}

func New() Set {
	return &set{
		mu:   &sync.RWMutex{},
		data: make(map[interface{}]struct{}),
	}
}

func (s *set) Values() []interface{} {
	var result []interface{}
	s.Range(func(v interface{}) bool {
		result = append(result, v)
		return false
	})

	return result
}

func (s *set) Add(v interface{}) bool {
	s.mu.RLock()
	_, found := s.data[v]
	if found {
		s.mu.RUnlock()
		return false
	}
	s.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[v] = struct{}{}
	return true
}

func (s *set) Delete(v interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, v)
}

func (s *set) Contains(vals ...interface{}) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, v := range vals {
		if _, ok := s.data[v]; !ok {
			return false
		}
	}

	return true
}

func (s *set) Len() uint {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return uint(len(s.data))
}

func (s *set) Range(fn func(interface{}) bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for v := range s.data {
		if fn(v) {
			break
		}
	}
}
