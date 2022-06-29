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
	Values() []interface{}
	Add(interface{}) bool
	Delete(interface{})
	Contains(...interface{}) bool
	Len() uint
	Range(func(interface{}) bool)
	Clear()
}

type set map[interface{}]struct{}

func New() Set {
	return &set{}
}

func (s *set) Values() []interface{} {
	var result []interface{}
	s.Range(func(v interface{}) bool {
		result = append(result, v)
		return true
	})

	return result
}

func (s *set) Add(v interface{}) bool {
	_, found := (*s)[v]
	if found {
		return false
	}

	(*s)[v] = struct{}{}
	return true
}

func (s *set) Delete(v interface{}) {
	delete(*s, v)
}

func (s *set) Contains(vals ...interface{}) bool {
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

func (s *set) Range(fn func(interface{}) bool) {
	for v := range *s {
		if !fn(v) {
			break
		}
	}
}

func (s *set) Clear() {
	*s = set{}
}
