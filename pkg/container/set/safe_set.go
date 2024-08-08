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

//go:generate mockgen -destination mocks/safe_set_mock.go -source safe_set.go -package mocks

package set

import (
	"sync"
)

type SafeSet[T comparable] interface {
	Values() []T
	Add(T) bool
	Delete(T)
	Contains(...T) bool
	Len() uint
	Clear()
}

type safeSet[T comparable] struct {
	mu   *sync.RWMutex
	data map[T]struct{}
}

func NewSafeSet[T comparable]() SafeSet[T] {
	return &safeSet[T]{
		mu:   &sync.RWMutex{},
		data: make(map[T]struct{}),
	}
}

func (s *safeSet[T]) Values() []T {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.data) == 0 {
		return nil
	}

	result := make([]T, 0, len(s.data))
	for k := range s.data {
		result = append(result, k)
	}

	return result
}

func (s *safeSet[T]) Add(v T) bool {
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

func (s *safeSet[T]) Delete(v T) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, v)
}

func (s *safeSet[T]) Contains(vals ...T) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, v := range vals {
		if _, ok := s.data[v]; !ok {
			return false
		}
	}

	return true
}

func (s *safeSet[T]) Len() uint {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return uint(len(s.data))
}

func (s *safeSet[T]) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = make(map[T]struct{})
}
