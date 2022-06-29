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

//go:generate mockgen -destination sorted_unique_list_mock.go -source sorted_unique_list.go -package list

package list

import (
	"sync"

	"d7y.io/dragonfly/v2/pkg/container/set"
)

type SortedUniqueList interface {
	Len() int
	Insert(Item)
	Remove(Item)
	Contains(Item) bool
	Range(func(Item) bool)
	ReverseRange(fn func(Item) bool)
}

type sortedUniqueList struct {
	mu        *sync.RWMutex
	container SortedList
	data      set.Set
}

func NewSortedUniqueList() SortedUniqueList {
	return &sortedUniqueList{
		mu:        &sync.RWMutex{},
		container: NewSortedList(),
		data:      set.New(),
	}
}

func (ul *sortedUniqueList) Len() int {
	ul.mu.RLock()
	defer ul.mu.RUnlock()

	return ul.container.Len()
}

func (ul *sortedUniqueList) Insert(item Item) {
	ul.mu.Lock()
	defer ul.mu.Unlock()

	if ok := ul.data.Contains(item); ok {
		ul.container.Remove(item)
		ul.container.Insert(item)
		return
	}

	ul.data.Add(item)
	ul.container.Insert(item)
}

func (ul *sortedUniqueList) Remove(item Item) {
	ul.mu.Lock()
	defer ul.mu.Unlock()

	ul.data.Delete(item)
	ul.container.Remove(item)
}

func (ul *sortedUniqueList) Contains(item Item) bool {
	ul.mu.RLock()
	defer ul.mu.RUnlock()

	return ul.data.Contains(item)
}

func (ul *sortedUniqueList) Range(fn func(item Item) bool) {
	ul.mu.RLock()
	defer ul.mu.RUnlock()

	ul.container.Range(func(item Item) bool {
		if !fn(item) {
			return false
		}
		return true
	})
}

func (ul *sortedUniqueList) ReverseRange(fn func(item Item) bool) {
	ul.mu.RLock()
	defer ul.mu.RUnlock()

	ul.container.ReverseRange(func(item Item) bool {
		if !fn(item) {
			return false
		}
		return true
	})
}
