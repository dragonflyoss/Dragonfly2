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

type Set interface {
	Values() []any
	Add(any) bool
	Delete(any)
	Contains(...any) bool
	Len() uint
	Range(func(any) bool)
	Clear()
}

type set map[any]struct{}

func New() Set {
	return &set{}
}

func (s *set) Values() []any {
	var result []any
	s.Range(func(v any) bool {
		result = append(result, v)
		return true
	})

	return result
}

func (s *set) Add(v any) bool {
	_, found := (*s)[v]
	if found {
		return false
	}

	(*s)[v] = struct{}{}
	return true
}

func (s *set) Delete(v any) {
	delete(*s, v)
}

func (s *set) Contains(vals ...any) bool {
	for _, v := range vals {
		if _, ok := (*s)[v]; !ok {
			return false
		}
	}

	return true
}

func (s *set) Len() uint {
	return uint(len(*s))
}

func (s *set) Range(fn func(any) bool) {
	for v := range *s {
		if !fn(v) {
			break
		}
	}
}

func (s *set) Clear() {
	*s = set{}
}
