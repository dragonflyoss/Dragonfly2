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
	data []set.Set
}

func NewBucket(len uint) Bucket {
	data := make([]set.Set, len)
	for i := range data {
		data[i] = set.New()
	}

	return &bucket{
		data: data,
	}
}

func (b *bucket) Add(i uint, v interface{}) bool {
	if b.Len() <= i {
		return false
	}

	if ok := b.data[i].Add(v); !ok {
		return false
	}

	return true
}

func (b *bucket) Delete(i uint, v interface{}) {
	if b.Len() <= i {
		return
	}

	b.data[i].Delete(v)
}

func (b *bucket) Contains(i uint, v interface{}) bool {
	if b.Len() <= i {
		return false
	}

	if ok := b.data[i].Contains(v); !ok {
		return false
	}

	return true
}

func (b *bucket) Len() uint {
	return uint(len(b.data))
}

func (b *bucket) Range(fn func(interface{}) bool) {
	for _, s := range b.data {
		if s.Len() > 0 {
			for _, v := range s.Values() {
				if fn(v) {
					return
				}
			}
		}
	}
}

func (b *bucket) ReverseRange(fn func(interface{}) bool) {
	for i := b.Len() - 1; i >= 0; i-- {
		s := b.data[i]
		if s.Len() > 0 {
			for _, v := range s.Values() {
				if fn(v) {
					return
				}
			}
		}
	}
}
