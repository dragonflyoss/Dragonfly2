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
	"math/rand"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

const N = 1000

func TestBucketAdd(t *testing.T) {
	tests := []struct {
		name   string
		value  interface{}
		index  uint
		len    uint
		expect func(t *testing.T, ok bool, b Bucket, index uint, value interface{})
	}{
		{
			name:  "add value succeeded",
			value: "foo",
			index: 0,
			len:   1,
			expect: func(t *testing.T, ok bool, b Bucket, index uint, value interface{}) {
				assert := assert.New(t)
				assert.Equal(ok, true)
				assert.Equal(b.Contains(index, value), true)
			},
		},
		{
			name:  "add value failed",
			value: "foo",
			index: 0,
			len:   1,
			expect: func(t *testing.T, _ bool, b Bucket, index uint, value interface{}) {
				assert := assert.New(t)
				ok := b.Add(index, value)
				assert.Equal(ok, false)
				assert.Equal(b.Contains(index, value), true)
			},
		},
		{
			name:  "add multi values",
			value: "foo",
			index: 0,
			len:   2,
			expect: func(t *testing.T, _ bool, b Bucket, index uint, value interface{}) {
				assert := assert.New(t)
				ok := b.Add(1, "bar")
				assert.Equal(ok, true)
				assert.Equal(b.Contains(index, value), true)
				assert.Equal(b.Contains(1, "bar"), true)
			},
		},
		{
			name:  "add value exceeds length",
			value: "foo",
			index: 1,
			len:   0,
			expect: func(t *testing.T, ok bool, b Bucket, index uint, value interface{}) {
				assert := assert.New(t)
				assert.Equal(ok, false)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := NewBucket(tc.len)
			tc.expect(t, b.Add(tc.index, tc.value), b, tc.index, tc.value)
		})
	}
}

func TestBucketAdd_Concurrent(t *testing.T) {
	runtime.GOMAXPROCS(2)

	b := NewBucket(N)
	nums := rand.Perm(N)

	var wg sync.WaitGroup
	wg.Add(len(nums))
	for i := 0; i < len(nums); i++ {
		go func(i int) {
			b.Add(uint(i), i)
			wg.Done()
		}(i)
	}

	wg.Wait()
	for _, n := range nums {
		if !b.Contains(uint(n), n) {
			t.Errorf("Bucket is missing element: %v", n)
		}
	}
}

func TestBucketDelete(t *testing.T) {
	tests := []struct {
		name   string
		value  interface{}
		index  uint
		len    uint
		expect func(t *testing.T, b Bucket, index uint, value interface{})
	}{
		{
			name:  "delete value succeeded",
			value: "foo",
			index: 0,
			len:   1,
			expect: func(t *testing.T, b Bucket, index uint, value interface{}) {
				assert := assert.New(t)
				b.Delete(index, value)
				assert.Equal(b.Contains(index, value), false)
			},
		},
		{
			name:  "delete value does not exist",
			value: "foo",
			index: 0,
			len:   1,
			expect: func(t *testing.T, b Bucket, index uint, value interface{}) {
				assert := assert.New(t)
				b.Delete(index, "bar")
				assert.Equal(b.Contains(index, value), true)
			},
		},
		{
			name:  "delete value exceeds length",
			value: "foo",
			index: 0,
			len:   1,
			expect: func(t *testing.T, b Bucket, index uint, value interface{}) {
				assert := assert.New(t)
				b.Delete(1, "bar")
				assert.Equal(b.Contains(index, value), true)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := NewBucket(tc.len)
			b.Add(tc.index, tc.value)
			tc.expect(t, b, tc.index, tc.value)
		})
	}
}

func TestSetDelete_Concurrent(t *testing.T) {
	runtime.GOMAXPROCS(2)

	b := NewBucket(N)
	nums := rand.Perm(N)
	for _, v := range nums {
		b.Add(uint(v), v)
	}

	var wg sync.WaitGroup
	wg.Add(len(nums))
	for _, v := range nums {
		go func(i int) {
			b.Delete(uint(i), i)
			wg.Done()
		}(v)
	}
	wg.Wait()

	for _, v := range nums {
		if ok := b.Contains(uint(v), v); ok {
			t.Errorf("Contains error %v", v)
		}
	}
}

func TestBucketContains(t *testing.T) {
	tests := []struct {
		name   string
		value  interface{}
		index  uint
		len    uint
		expect func(t *testing.T, b Bucket, index uint, value interface{})
	}{
		{
			name:  "contains value succeeded",
			value: "foo",
			index: 0,
			len:   1,
			expect: func(t *testing.T, b Bucket, index uint, value interface{}) {
				assert := assert.New(t)
				assert.Equal(b.Contains(index, value), true)
			},
		},
		{
			name:  "contains value does not exist",
			value: "foo",
			index: 0,
			len:   1,
			expect: func(t *testing.T, b Bucket, index uint, _ interface{}) {
				assert := assert.New(t)
				assert.Equal(b.Contains(index, "bar"), false)
			},
		},
		{
			name:  "contains value exceeds length",
			value: "foo",
			index: 0,
			len:   1,
			expect: func(t *testing.T, b Bucket, _ uint, _ interface{}) {
				assert := assert.New(t)
				assert.Equal(b.Contains(2, "bar"), false)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := NewBucket(tc.len)
			b.Add(tc.index, tc.value)
			tc.expect(t, b, tc.index, tc.value)
		})
	}
}

func TestBucketContains_Concurrent(t *testing.T) {
	runtime.GOMAXPROCS(2)

	b := NewBucket(N)
	nums := rand.Perm(N)
	for _, v := range nums {
		b.Add(uint(v), v)
	}

	var wg sync.WaitGroup
	for _, v := range nums {
		wg.Add(1)
		go func(i int) {
			b.Contains(uint(i), i)
			wg.Done()
		}(v)
	}
	wg.Wait()
}

func TestBucketRange(t *testing.T) {
	tests := []struct {
		name   string
		len    uint
		expect func(t *testing.T, b Bucket)
	}{
		{
			name: "range succeeded",
			len:  10,
			expect: func(t *testing.T, b Bucket) {
				assert := assert.New(t)
				b.Add(0, "foo")
				b.Range(func(v interface{}) bool {
					assert.Equal(v, "foo")
					return false
				})
			},
		},
		{
			name: "range failed",
			len:  10,
			expect: func(t *testing.T, b Bucket) {
				assert := assert.New(t)
				b.Add(0, "foo")
				b.Add(1, "bar")
				b.Range(func(v interface{}) bool {
					assert.Equal(v, "foo")
					return true
				})
			},
		},
		{
			name: "range multi values",
			len:  10,
			expect: func(t *testing.T, b Bucket) {
				assert := assert.New(t)
				count := 0
				b.Add(0, "foo")
				b.Add(0, "bar")
				b.Range(func(v interface{}) bool {
					assert.Contains([]string{"foo", "bar"}, "foo")
					count++
					return true
				})
				assert.Equal(count, 2)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := NewBucket(tc.len)
			tc.expect(t, s)
		})
	}
}

func TestBucketRange_Concurrent(t *testing.T) {
	runtime.GOMAXPROCS(2)

	b := NewBucket(N)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		i := 0
		b.Range(func(v interface{}) bool {
			i++
			return false
		})

		j := 0
		b.Range(func(v interface{}) bool {
			j++
			return false
		})
		if j < i {
			t.Errorf("Values shrunk from %v to %v", i, j)
		}
		wg.Done()
	}()

	for i := 0; i < N; i++ {
		b.Add(uint(i), i)
	}
	wg.Wait()
}

func TestBucketReverseRange(t *testing.T) {
	tests := []struct {
		name   string
		len    uint
		expect func(t *testing.T, b Bucket)
	}{
		{
			name: "reverse range succeeded",
			len:  10,
			expect: func(t *testing.T, b Bucket) {
				assert := assert.New(t)
				b.Add(0, "foo")
				b.Add(0, "bar")
				b.Add(1, "baz")

				i := 0
				b.ReverseRange(func(v interface{}) bool {
					if i == 0 {
						assert.Equal(v, "baz")
					} else {
						assert.Contains([]string{"foo", "bar"}, v)
					}

					i++
					return false
				})
			},
		},
		{
			name: "reverse range failed",
			len:  10,
			expect: func(t *testing.T, b Bucket) {
				assert := assert.New(t)
				b.Add(0, "foo")
				b.Add(1, "bar")
				b.ReverseRange(func(v interface{}) bool {
					assert.Equal(v, "bar")
					return true
				})
			},
		},
		{
			name: "reverse range multi values",
			len:  10,
			expect: func(t *testing.T, b Bucket) {
				assert := assert.New(t)
				count := 0
				b.Add(0, "foo")
				b.Add(0, "bar")
				b.ReverseRange(func(v interface{}) bool {
					assert.Contains([]string{"foo", "bar"}, "foo")
					count++
					return true
				})
				assert.Equal(count, 2)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := NewBucket(tc.len)
			tc.expect(t, s)
		})
	}
}

func TestBucketReverseRange_Concurrent(t *testing.T) {
	runtime.GOMAXPROCS(2)

	b := NewBucket(N)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		i := 0
		b.ReverseRange(func(v interface{}) bool {
			i++
			return false
		})

		j := 0
		b.ReverseRange(func(v interface{}) bool {
			j++
			return false
		})
		if i < j {
			t.Errorf("Values shrunk from %v to %v", i, j)
		}
		wg.Done()
	}()

	for i := 0; i < N; i++ {
		b.Add(uint(i), i)
	}
	wg.Wait()
}
