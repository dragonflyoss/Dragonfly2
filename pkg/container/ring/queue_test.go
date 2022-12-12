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

package ring

import (
	"testing"

	testifyassert "github.com/stretchr/testify/assert"
	"golang.org/x/exp/slices"
)

func TestSequence(t *testing.T) {
	var testCases = []struct {
		name     string
		exponent int
		values   []int
	}{
		{
			name:     "sequence with exponent 1",
			exponent: 1,
			values:   []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			name:     "sequence with exponent 2",
			exponent: 2,
			values:   []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			name:     "sequence with exponent 4",
			exponent: 4,
			values:   []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			name:     "random with exponent 1",
			exponent: 1,
			values:   []int{8, 5, 1, 3, 7, 4, 2, 6, 9, 0},
		},
		{
			name:     "random with exponent 2",
			exponent: 2,
			values:   []int{8, 5, 1, 3, 7, 4, 2, 6, 9, 0},
		},
		{
			name:     "random with exponent 4",
			exponent: 4,
			values:   []int{8, 5, 1, 3, 7, 4, 2, 6, 9, 0},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert := testifyassert.New(t)

			q := NewSequence[int](tc.exponent)
			go func() {
				for _, v := range tc.values {
					var vv int
					vv = v
					q.Enqueue(&vv)
				}
			}()
			var values []int
			for i := 0; i < len(tc.values); i++ {
				val, ok := q.Dequeue()
				assert.True(ok, "dequeue should be ok")
				values = append(values, *val)
			}
			assert.Equal(tc.values, values)

			q.Close()
			_, ok := q.Dequeue()
			assert.False(ok, "dequeue after closed should be false")
		})
	}
}

func TestRandom(t *testing.T) {
	var testCases = []struct {
		name     string
		exponent int
		values   []int
	}{
		{
			name:     "exponent 1 - case 1",
			exponent: 1,
			values:   []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			name:     "exponent 2 - case 1",
			exponent: 2,
			values:   []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			name:     "exponent 4 - case 1",
			exponent: 4,
			values:   []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			name:     "exponent 1 - case 2",
			exponent: 1,
			values:   []int{8, 5, 1, 3, 7, 4, 2, 6, 9, 0},
		},
		{
			name:     "exponent 2 - case 2",
			exponent: 2,
			values:   []int{8, 5, 1, 3, 7, 4, 2, 6, 9, 0},
		},
		{
			name:     "exponent 4 - case 2",
			exponent: 4,
			values:   []int{8, 5, 1, 3, 7, 4, 2, 6, 9, 0},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert := testifyassert.New(t)

			q := NewRandom[int](tc.exponent)
			go func() {
				for _, v := range tc.values {
					var vv int
					vv = v
					q.Enqueue(&vv)
				}
			}()
			var values []int
			for i := 0; i < len(tc.values); i++ {
				val, ok := q.Dequeue()
				assert.True(ok, "dequeue should be ok")
				values = append(values, *val)
			}
			slices.Sort(tc.values)
			slices.Sort(values)
			assert.Equal(tc.values, values)

			q.Close()
			_, ok := q.Dequeue()
			assert.False(ok, "dequeue after closed should be false")
		})
	}
}

func benchmarkRandom(b *testing.B, exponent int, input, output int) {
	queue := NewRandom[int](exponent)
	done := false
	for i := 0; i < input; i++ {
		go func(i int) {
			for {
				if done {
					return
				}
				queue.Enqueue(&i)
			}
		}(i)
	}
	for i := 0; i < b.N; i++ {
		queue.Dequeue()
	}
	queue.Close()
	done = true
}

func BenchmarkRandomExpo1Input2(b *testing.B) {
	benchmarkRandom(b, 1, 2, 0)
}
func BenchmarkRandomExpo1Input4(b *testing.B) {
	benchmarkRandom(b, 1, 4, 0)
}
func BenchmarkRandomExpo1Input8(b *testing.B) {
	benchmarkRandom(b, 1, 8, 0)
}
func BenchmarkRandomExpo2Input2(b *testing.B) {
	benchmarkRandom(b, 2, 2, 0)
}
func BenchmarkRandomExpo2Input4(b *testing.B) {
	benchmarkRandom(b, 2, 4, 0)
}
func BenchmarkRandomExpo2Input8(b *testing.B) {
	benchmarkRandom(b, 2, 8, 0)
}
func BenchmarkRandomExpo3Input2(b *testing.B) {
	benchmarkRandom(b, 3, 2, 0)
}
func BenchmarkRandomExpo3Input4(b *testing.B) {
	benchmarkRandom(b, 3, 4, 0)
}
func BenchmarkRandomExpo3Input8(b *testing.B) {
	benchmarkRandom(b, 3, 8, 0)
}
