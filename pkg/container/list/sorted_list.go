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

//go:generate mockgen -destination sorted_list_mock.go -source sorted_list.go -package list

package list

import (
	"container/list"
	"sync"
)

type Item interface {
	SortedValue() int
}

type SortedList interface {
	Len() int
	Insert(Item)
	Remove(Item)
	Contains(Item) bool
	Range(func(Item) bool)
	ReverseRange(fn func(Item) bool)
}

type sortedList struct {
	mu        *sync.RWMutex
	container *list.List
}

func NewSortedList() SortedList {
	return &sortedList{
		mu:        &sync.RWMutex{},
		container: list.New(),
	}
}

func (l *sortedList) Len() int {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.container.Len()
}

func (l *sortedList) Insert(item Item) {
	l.mu.Lock()
	defer l.mu.Unlock()

	for e := l.container.Front(); e != nil; e = e.Next() {
		v, ok := e.Value.(Item)
		if !ok {
			continue
		}

		if v.SortedValue() >= item.SortedValue() {
			l.container.InsertBefore(item, e)
			return
		}
	}

	l.container.PushBack(item)
}

func (l *sortedList) Remove(item Item) {
	l.mu.Lock()
	defer l.mu.Unlock()

	for e := l.container.Front(); e != nil; e = e.Next() {
		v, ok := e.Value.(Item)
		if !ok {
			continue
		}

		if v == item {
			l.container.Remove(e)
			return
		}
	}
}

func (l *sortedList) Contains(item Item) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()

	for e := l.container.Front(); e != nil; e = e.Next() {
		if v, ok := e.Value.(Item); ok && v == item {
			return true
		}
	}

	return false
}

func (l *sortedList) Range(fn func(Item) bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	for e := l.container.Front(); e != nil; e = e.Next() {
		v, ok := e.Value.(Item)
		if !ok {
			continue
		}

		if !fn(v) {
			return
		}
	}
}

func (l *sortedList) ReverseRange(fn func(Item) bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	for e := l.container.Back(); e != nil; e = e.Prev() {
		v, ok := e.Value.(Item)
		if !ok {
			continue
		}

		if !fn(v) {
			return
		}
	}
}
