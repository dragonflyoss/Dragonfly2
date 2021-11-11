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

type Set interface {
	Values() []string
	Add(string) bool
	Delete(string)
	Contains(...string) bool
	Len() uint
	Range(func(string) bool)
}

type set struct {
	data map[string]struct{}
}

func NewSet() Set {
	return &set{
		data: make(map[string]struct{}),
	}
}

func (s *set) Values() []string {
	var result []string
	s.Range(func(v string) bool {
		result = append(result, v)
		return true
	})

	return result
}

func (s *set) Add(v string) bool {
	_, found := s.data[v]
	if found {
		return false
	}

	s.data[v] = struct{}{}
	return true
}

func (s *set) Delete(v string) {
	delete(s.data, v)
}

func (s *set) Contains(vals ...string) bool {
	for _, v := range vals {
		if _, ok := s.data[v]; !ok {
			return false
		}
	}

	return true
}

func (s *set) Len() uint {
	return uint(len(s.data))
}

func (s *set) Range(fn func(string) bool) {
	for v := range s.data {
		if !fn(v) {
			break
		}
	}
}
