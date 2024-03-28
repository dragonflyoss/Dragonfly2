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

//go:generate mockgen -destination mocks/set_mock.go -source set.go -package mocks

package set

type Set[T comparable] interface {
	Values() []T
	Add(T) bool
	Delete(T)
	Contains(...T) bool
	Len() uint
	Clear()
}

type set[T comparable] map[T]struct{}

func New[T comparable]() Set[T] {
	return &set[T]{}
}

func (s *set[T]) Values() []T {
	if len(*s) == 0 {
		return nil
	}

	result := make([]T, 0, len(*s))
	for k := range *s {
		result = append(result, k)
	}

	return result
}

func (s *set[T]) Add(v T) bool {
	_, found := (*s)[v]
	if found {
		return false
	}

	(*s)[v] = struct{}{}
	return true
}

func (s *set[T]) Delete(v T) {
	delete(*s, v)
}

func (s *set[T]) Contains(vals ...T) bool {
	for _, v := range vals {
		if _, ok := (*s)[v]; !ok {
			return false
		}
	}

	return true
}

func (s *set[T]) Len() uint {
	return uint(len(*s))
}

func (s *set[T]) Clear() {
	*s = set[T]{}
}
