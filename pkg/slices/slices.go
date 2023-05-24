/*
 *     Copyright 2022 The Dragonfly Authors
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

package slices

// Contains returns true if an element is present in a collection.
func Contains[T comparable](s []T, e T) bool {
	for _, v := range s {
		if v == e {
			return true
		}
	}

	return false
}

// FindDuplicate returns duplicate element in a collection.
func FindDuplicate[T comparable](s []T) (T, bool) {
	visited := make(map[T]struct{})
	for _, v := range s {
		if _, ok := visited[v]; ok {
			return v, true
		}

		visited[v] = struct{}{}
	}

	var zero T
	return zero, false
}

// RemoveDuplicates removes duplicate element in a collection.
func RemoveDuplicates[T comparable](s []T) []T {
	var result []T
	visited := make(map[T]bool, len(s))
	for _, v := range s {
		if !visited[v] {
			visited[v] = true
			result = append(result, v)
		}
	}

	return result
}

// Reverse reverses elements in a collection.
func Reverse[S ~[]T, T any](s S) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}
