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

import (
	"reflect"
	"testing"
)

func TestContains(t *testing.T) {
	tests := []struct {
		name     string
		input    []int
		element  int
		expected bool
	}{
		{
			name:     "element present",
			input:    []int{1, 2, 3},
			element:  2,
			expected: true,
		},
		{
			name:     "element not present",
			input:    []int{1, 2, 3},
			element:  4,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Contains(tt.input, tt.element)
			if result != tt.expected {
				t.Errorf("expected %v, but got %v", tt.expected, result)
			}
		})
	}
}

func TestFindDuplicate(t *testing.T) {
	tests := []struct {
		name     string
		input    []int
		expected int
		found    bool
	}{
		{
			name:     "duplicate present",
			input:    []int{1, 2, 3, 2},
			expected: 2,
			found:    true,
		},
		{
			name:     "duplicate not present",
			input:    []int{1, 2, 3},
			expected: 0,
			found:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, found := FindDuplicate(tt.input)
			if found != tt.found {
				t.Errorf("expected found to be %v, but got %v", tt.found, found)
			}
			if result != tt.expected {
				t.Errorf("expected %v, but got %v", tt.expected, result)
			}
		})
	}
}

func TestRemoveDuplicates(t *testing.T) {
	tests := []struct {
		name     string
		input    []int
		expected []int
	}{
		{
			name:     "no duplicates",
			input:    []int{1, 2, 3},
			expected: []int{1, 2, 3},
		},
		{
			name:     "with duplicates",
			input:    []int{1, 2, 3, 2},
			expected: []int{1, 2, 3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := RemoveDuplicates(tt.input)
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("expected %v, but got %v", tt.expected, result)
			}
		})
	}
}

func TestReverse(t *testing.T) {
	tests := []struct {
		name     string
		input    []int
		expected []int
	}{
		{
			name:     "even number of elements",
			input:    []int{1, 2, 3, 4},
			expected: []int{4, 3, 2, 1},
		},
		{
			name:     "odd number of elements",
			input:    []int{1, 2, 3},
			expected: []int{3, 2, 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Reverse(tt.input)
			if !reflect.DeepEqual(tt.input, tt.expected) {
				t.Errorf("expected %v, but got %v", tt.expected, tt.input)
			}
		})
	}
}
