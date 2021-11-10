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

//go:generate mockgen -destination ./mocks/sorted_map_mock.go -package mocks d7y.io/dragonfly/v2/pkg/sortedmap Item

package sortedmap

import (
	"errors"
	"sync"
)

type Item interface {
	SortedValue() uint
}

type SortedMap interface {
	Add(string, Item) error
	Update(string, Item) error
	Delete(string) error
	Range(func(string, Item) bool)
	ReverseRange(func(string, Item) bool)
	Len() uint
}

type sortedMap struct {
	mu        *sync.RWMutex
	bucket    Bucket
	data      map[string]Item
	bucketLen uint
}

func New(bucketLen uint) SortedMap {
	return &sortedMap{
		mu:        &sync.RWMutex{},
		bucket:    NewBucket(bucketLen),
		data:      map[string]Item{},
		bucketLen: bucketLen,
	}
}

func (s *sortedMap) Add(key string, item Item) error {
	if item.SortedValue() > s.bucketLen-1 {
		return errors.New("sorted value is illegal")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	oldItem, ok := s.data[key]
	if ok {
		oldBkey := oldItem.SortedValue()
		s.bucket.Delete(oldBkey, key)

		bkey := item.SortedValue()
		s.bucket.Add(bkey, key)
		s.data[key] = item
		return nil
	}

	bKey := item.SortedValue()
	s.bucket.Add(bKey, key)
	s.data[key] = item
	return nil
}

func (s *sortedMap) Update(key string, item Item) error {
	if item.SortedValue() > s.bucketLen-1 {
		return errors.New("sorted value is illegal")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	oldItem, ok := s.data[key]
	if !ok {
		return errors.New("key does not exist")
	}

	oldBkey := oldItem.SortedValue()
	s.bucket.Delete(oldBkey, key)

	bkey := item.SortedValue()
	s.bucket.Add(bkey, key)
	s.data[key] = item
	return nil
}

func (s *sortedMap) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.data[key]
	if !ok {
		return errors.New("key does not exist")
	}

	bkey := item.SortedValue()
	s.bucket.Delete(bkey, key)
	delete(s.data, key)
	return nil
}

func (s *sortedMap) Range(fn func(key string, item Item) bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.bucket.Range(func(v interface{}) bool {
		if k, ok := v.(string); ok {
			if item, ok := s.data[k]; ok {
				if !fn(k, item) {
					return false
				}
			}
		}

		return true
	})
}

func (s *sortedMap) ReverseRange(fn func(key string, item Item) bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.bucket.ReverseRange(func(v interface{}) bool {
		if k, ok := v.(string); ok {
			if item, ok := s.data[k]; ok {
				if !fn(k, item) {
					return false
				}
			}
		}

		return true
	})
}

func (s *sortedMap) Len() uint {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return uint(len(s.data))
}
