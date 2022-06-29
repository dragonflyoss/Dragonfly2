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

package list

import (
	"math/rand"
	"runtime"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

const N = 1000

func TestSortedListInsert(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(m ...*MockItemMockRecorder)
		expect func(t *testing.T, l SortedList, items ...Item)
	}{
		{
			name: "insert values",
			mock: func(m ...*MockItemMockRecorder) {},
			expect: func(t *testing.T, l SortedList, items ...Item) {
				assert := assert.New(t)
				l.Insert(items[0])
				assert.Equal(l.Contains(items[0]), true)
				assert.Equal(l.Len(), 1)
			},
		},
		{
			name: "insert multi value",
			mock: func(m ...*MockItemMockRecorder) {
				gomock.InOrder(
					m[0].SortedValue().Return(0).Times(1),
					m[1].SortedValue().Return(1).Times(1),
				)
			},
			expect: func(t *testing.T, l SortedList, items ...Item) {
				assert := assert.New(t)
				l.Insert(items[0])
				l.Insert(items[1])
				assert.Equal(l.Contains(items[0]), true)
				assert.Equal(l.Contains(items[1]), true)
				assert.Equal(l.Len(), 2)
			},
		},
		{
			name: "insert same values",
			mock: func(m ...*MockItemMockRecorder) {
				gomock.InOrder(
					m[0].SortedValue().Return(0).Times(2),
				)
			},
			expect: func(t *testing.T, l SortedList, items ...Item) {
				assert := assert.New(t)
				l.Insert(items[0])
				l.Insert(items[0])
				assert.Equal(l.Contains(items[0]), true)
				assert.Equal(l.Len(), 2)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			mockItems := []*MockItem{NewMockItem(ctl), NewMockItem(ctl)}
			tc.mock(mockItems[0].EXPECT(), mockItems[1].EXPECT())
			tc.expect(t, NewSortedList(), mockItems[0], mockItems[1])
		})
	}
}

func TestSortedListInsert_Concurrent(t *testing.T) {
	runtime.GOMAXPROCS(2)

	ctl := gomock.NewController(t)
	defer ctl.Finish()
	mockItem := NewMockItem(ctl)
	mockItem.EXPECT().SortedValue().DoAndReturn(func() int { return rand.Intn(N) }).AnyTimes()

	l := NewSortedList()
	nums := rand.Perm(N)

	var wg sync.WaitGroup
	wg.Add(len(nums))
	for i := 0; i < len(nums); i++ {
		go func(i int) {
			l.Insert(mockItem)
			wg.Done()
		}(i)
	}

	wg.Wait()
	count := 0
	l.Range(func(item Item) bool {
		count++
		return true
	})
	if count != len(nums) {
		t.Errorf("SortedList is missing element")
	}
}

func TestSortedListRemove(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(m ...*MockItemMockRecorder)
		expect func(t *testing.T, l SortedList, items ...Item)
	}{
		{
			name: "remove values",
			mock: func(m ...*MockItemMockRecorder) {
				gomock.InOrder(
					m[0].SortedValue().Return(0).Times(1),
					m[1].SortedValue().Return(1).Times(1),
				)
			},
			expect: func(t *testing.T, l SortedList, items ...Item) {
				assert := assert.New(t)
				l.Insert(items[0])
				l.Insert(items[1])
				assert.Equal(l.Contains(items[0]), true)
				assert.Equal(l.Contains(items[1]), true)
				assert.Equal(l.Len(), 2)
				l.Remove(items[0])
				assert.Equal(l.Contains(items[0]), false)
				assert.Equal(l.Len(), 1)
				l.Remove(items[1])
				assert.Equal(l.Contains(items[1]), false)
				assert.Equal(l.Len(), 0)
			},
		},
		{
			name: "remove value dost not exits",
			mock: func(m ...*MockItemMockRecorder) {},
			expect: func(t *testing.T, l SortedList, items ...Item) {
				assert := assert.New(t)
				l.Insert(items[0])
				assert.Equal(l.Contains(items[0]), true)
				assert.Equal(l.Len(), 1)
				l.Remove(items[1])
				assert.Equal(l.Contains(items[0]), true)
				assert.Equal(l.Len(), 1)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			mockItems := []*MockItem{NewMockItem(ctl), NewMockItem(ctl)}
			tc.mock(mockItems[0].EXPECT(), mockItems[1].EXPECT())
			tc.expect(t, NewSortedList(), mockItems[0], mockItems[1])
		})
	}
}

func TestSortedListRemove_Concurrent(t *testing.T) {
	runtime.GOMAXPROCS(2)

	ctl := gomock.NewController(t)
	defer ctl.Finish()
	mockItem := NewMockItem(ctl)
	mockItem.EXPECT().SortedValue().DoAndReturn(func() int { return rand.Intn(N) }).AnyTimes()

	l := NewSortedList()
	nums := rand.Perm(N)

	for i := 0; i < len(nums); i++ {
		l.Insert(mockItem)
	}

	var wg sync.WaitGroup
	wg.Add(len(nums))
	for i := 0; i < len(nums); i++ {
		go func(i int) {
			l.Remove(mockItem)
			wg.Done()
		}(i)
	}

	wg.Wait()
	if l.Len() != 0 {
		t.Errorf("SortedList is redundant elements")
	}
}

func TestSortedListContains(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(m ...*MockItemMockRecorder)
		expect func(t *testing.T, l SortedList, items ...Item)
	}{
		{
			name: "contains values",
			mock: func(m ...*MockItemMockRecorder) {
				gomock.InOrder(
					m[0].SortedValue().Return(0).Times(1),
					m[1].SortedValue().Return(1).Times(1),
				)
			},
			expect: func(t *testing.T, l SortedList, items ...Item) {
				assert := assert.New(t)
				l.Insert(items[0])
				l.Insert(items[1])
				assert.Equal(l.Contains(items[0]), true)
				assert.Equal(l.Contains(items[1]), true)
			},
		},
		{
			name: "contains value dost not exits",
			mock: func(m ...*MockItemMockRecorder) {},
			expect: func(t *testing.T, l SortedList, items ...Item) {
				assert := assert.New(t)
				l.Insert(items[0])
				assert.Equal(l.Contains(items[1]), false)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			mockItems := []*MockItem{NewMockItem(ctl), NewMockItem(ctl)}
			tc.mock(mockItems[0].EXPECT(), mockItems[1].EXPECT())
			tc.expect(t, NewSortedList(), mockItems[0], mockItems[1])
		})
	}
}

func TestSortedListContains_Concurrent(t *testing.T) {
	runtime.GOMAXPROCS(2)

	ctl := gomock.NewController(t)
	defer ctl.Finish()
	mockItem := NewMockItem(ctl)
	mockItem.EXPECT().SortedValue().DoAndReturn(func() int { return rand.Intn(N) }).AnyTimes()

	l := NewSortedList()
	nums := rand.Perm(N)
	for range nums {
		l.Insert(mockItem)
	}

	var wg sync.WaitGroup
	for range nums {
		wg.Add(1)
		go func() {
			if ok := l.Contains(mockItem); !ok {
				t.Error("SortedList contains error")
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func TestSortedListLen(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(m ...*MockItemMockRecorder)
		expect func(t *testing.T, l SortedList, items ...Item)
	}{
		{
			name: "get length",
			mock: func(m ...*MockItemMockRecorder) {
				gomock.InOrder(
					m[0].SortedValue().Return(0).Times(1),
					m[1].SortedValue().Return(1).Times(1),
				)
			},
			expect: func(t *testing.T, l SortedList, items ...Item) {
				assert := assert.New(t)
				l.Insert(items[0])
				l.Insert(items[1])
				assert.Equal(l.Len(), 2)
			},
		},
		{
			name: "get empty list length",
			mock: func(m ...*MockItemMockRecorder) {},
			expect: func(t *testing.T, l SortedList, items ...Item) {
				assert := assert.New(t)
				assert.Equal(l.Len(), 0)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			mockItems := []*MockItem{NewMockItem(ctl), NewMockItem(ctl)}
			tc.mock(mockItems[0].EXPECT(), mockItems[1].EXPECT())
			tc.expect(t, NewSortedList(), mockItems[0], mockItems[1])
		})
	}
}

func TestSortedListLen_Concurrent(t *testing.T) {
	runtime.GOMAXPROCS(2)

	ctl := gomock.NewController(t)
	defer ctl.Finish()
	mockItem := NewMockItem(ctl)
	mockItem.EXPECT().SortedValue().DoAndReturn(func() int { return rand.Intn(N) }).AnyTimes()

	l := NewSortedList()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		elems := l.Len()
		for i := 0; i < N; i++ {
			newElems := l.Len()
			if newElems < elems {
				t.Errorf("Len shrunk from %v to %v", elems, newElems)
			}
		}
		wg.Done()
	}()

	for i := 0; i < N; i++ {
		l.Insert(mockItem)
	}
	wg.Wait()
}

func TestSortedListRange(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(m ...*MockItemMockRecorder)
		expect func(t *testing.T, l SortedList, items ...Item)
	}{
		{
			name: "range values",
			mock: func(m ...*MockItemMockRecorder) {
				gomock.InOrder(
					m[0].SortedValue().Return(0).Times(1),
					m[1].SortedValue().Return(1).Times(1),
				)
			},
			expect: func(t *testing.T, l SortedList, items ...Item) {
				assert := assert.New(t)
				l.Insert(items[0])
				l.Insert(items[1])
				assert.Equal(l.Len(), 2)

				i := 0
				l.Range(func(item Item) bool {
					assert.Equal(item, items[i])
					i++
					return true
				})
			},
		},
		{
			name: "range multi values",
			mock: func(m ...*MockItemMockRecorder) {
				for i := range m {
					m[i].SortedValue().Return(i).AnyTimes()
				}
			},
			expect: func(t *testing.T, l SortedList, items ...Item) {
				assert := assert.New(t)
				for _, item := range items {
					l.Insert(item)
				}
				assert.Equal(l.Len(), 10)

				i := 0
				l.Range(func(item Item) bool {
					assert.Equal(item, items[i])
					i++
					return true
				})
			},
		},
		{
			name: "range stoped",
			mock: func(m ...*MockItemMockRecorder) {
				gomock.InOrder(
					m[0].SortedValue().Return(0).Times(1),
					m[1].SortedValue().Return(1).Times(1),
				)
			},
			expect: func(t *testing.T, l SortedList, items ...Item) {
				assert := assert.New(t)
				l.Insert(items[0])
				l.Insert(items[1])
				assert.Equal(l.Len(), 2)

				l.Range(func(item Item) bool {
					assert.Equal(item, items[0])
					return false
				})
			},
		},
		{
			name: "range same values",
			mock: func(m ...*MockItemMockRecorder) {
				gomock.InOrder(
					m[0].SortedValue().Return(0).AnyTimes(),
				)
			},
			expect: func(t *testing.T, l SortedList, items ...Item) {
				assert := assert.New(t)
				l.Insert(items[0])
				l.Insert(items[0])
				l.Insert(items[0])
				assert.Equal(l.Len(), 3)

				count := 0
				l.Range(func(item Item) bool {
					assert.Equal(item, items[0])
					count++
					return true
				})
				assert.Equal(count, 3)
			},
		},
		{
			name: "range empty list",
			mock: func(m ...*MockItemMockRecorder) {
			},
			expect: func(t *testing.T, l SortedList, items ...Item) {
				assert := assert.New(t)
				count := 0
				l.Range(func(item Item) bool {
					count++
					return true
				})
				assert.Equal(count, 0)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			var mockItems []Item
			var mockItemRecorders []*MockItemMockRecorder
			for i := 0; i < 10; i++ {
				mockItem := NewMockItem(ctl)
				mockItemRecorders = append(mockItemRecorders, mockItem.EXPECT())
				mockItems = append(mockItems, mockItem)
			}

			tc.mock(mockItemRecorders...)
			tc.expect(t, NewSortedList(), mockItems...)
		})
	}
}

func TestSortedListRange_Concurrent(t *testing.T) {
	runtime.GOMAXPROCS(2)

	ctl := gomock.NewController(t)
	defer ctl.Finish()
	mockItem := NewMockItem(ctl)
	mockItem.EXPECT().SortedValue().DoAndReturn(func() int { return rand.Intn(N) }).AnyTimes()

	l := NewSortedList()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		i := 0
		l.Range(func(_ Item) bool {
			i++
			return true
		})

		j := 0
		l.Range(func(_ Item) bool {
			j++
			return true
		})
		if j < i {
			t.Errorf("Values shrunk from %v to %v", i, j)
		}
		wg.Done()
	}()

	for i := 0; i < N; i++ {
		l.Insert(mockItem)
	}
	wg.Wait()
}

func TestSortedListReverseRange(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(m ...*MockItemMockRecorder)
		expect func(t *testing.T, l SortedList, items ...Item)
	}{
		{
			name: "reverse range values",
			mock: func(m ...*MockItemMockRecorder) {
				gomock.InOrder(
					m[0].SortedValue().Return(0).Times(1),
					m[1].SortedValue().Return(1).Times(1),
				)
			},
			expect: func(t *testing.T, l SortedList, items ...Item) {
				assert := assert.New(t)
				l.Insert(items[0])
				l.Insert(items[1])
				assert.Equal(l.Len(), 2)

				i := 0
				l.ReverseRange(func(item Item) bool {
					assert.Equal(item, items[i])
					i++
					return true
				})
			},
		},
		{
			name: "reverse range multi values",
			mock: func(m ...*MockItemMockRecorder) {
				for i := range m {
					m[i].SortedValue().Return(i).AnyTimes()
				}
			},
			expect: func(t *testing.T, l SortedList, items ...Item) {
				assert := assert.New(t)
				for _, item := range items {
					l.Insert(item)
				}
				assert.Equal(l.Len(), 10)

				i := 9
				l.ReverseRange(func(item Item) bool {
					assert.Equal(item, items[i])
					i--
					return true
				})
			},
		},
		{
			name: "reverse range stoped",
			mock: func(m ...*MockItemMockRecorder) {
				gomock.InOrder(
					m[0].SortedValue().Return(0).Times(1),
					m[1].SortedValue().Return(1).Times(1),
				)
			},
			expect: func(t *testing.T, l SortedList, items ...Item) {
				assert := assert.New(t)
				l.Insert(items[0])
				l.Insert(items[1])
				assert.Equal(l.Len(), 2)

				l.ReverseRange(func(item Item) bool {
					assert.Equal(item, items[1])
					return false
				})
			},
		},
		{
			name: "reverse range same values",
			mock: func(m ...*MockItemMockRecorder) {
				gomock.InOrder(
					m[0].SortedValue().Return(0).AnyTimes(),
				)
			},
			expect: func(t *testing.T, l SortedList, items ...Item) {
				assert := assert.New(t)
				l.Insert(items[0])
				l.Insert(items[0])
				l.Insert(items[0])
				assert.Equal(l.Len(), 3)

				count := 0
				l.ReverseRange(func(item Item) bool {
					assert.Equal(item, items[0])
					count++
					return true
				})
				assert.Equal(count, 3)
			},
		},
		{
			name: "reverse range empty list",
			mock: func(m ...*MockItemMockRecorder) {
			},
			expect: func(t *testing.T, l SortedList, items ...Item) {
				assert := assert.New(t)
				count := 0
				l.ReverseRange(func(item Item) bool {
					count++
					return true
				})
				assert.Equal(count, 0)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			var mockItems []Item
			var mockItemRecorders []*MockItemMockRecorder
			for i := 0; i < 10; i++ {
				mockItem := NewMockItem(ctl)
				mockItemRecorders = append(mockItemRecorders, mockItem.EXPECT())
				mockItems = append(mockItems, mockItem)
			}

			tc.mock(mockItemRecorders...)
			tc.expect(t, NewSortedList(), mockItems...)
		})
	}
}

func TestSortedListReverseRange_Concurrent(t *testing.T) {
	runtime.GOMAXPROCS(2)

	ctl := gomock.NewController(t)
	defer ctl.Finish()
	mockItem := NewMockItem(ctl)
	mockItem.EXPECT().SortedValue().DoAndReturn(func() int { return rand.Intn(N) }).AnyTimes()

	l := NewSortedList()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		i := 0
		l.ReverseRange(func(_ Item) bool {
			i++
			return true
		})

		j := 0
		l.ReverseRange(func(_ Item) bool {
			j++
			return true
		})
		if j < i {
			t.Errorf("Values shrunk from %v to %v", i, j)
		}
		wg.Done()
	}()

	for i := 0; i < N; i++ {
		l.Insert(mockItem)
	}
	wg.Wait()
}

type item struct{ id int }

func (i *item) SortedValue() int { return rand.Intn(1000) }

func BenchmarkSortedListInsert(b *testing.B) {
	l := NewSortedList()

	var mockItems []*item
	for i := 0; i < b.N; i++ {
		mockItems = append(mockItems, &item{id: i})
	}

	b.ResetTimer()
	for _, mockItem := range mockItems {
		l.Insert(mockItem)
	}
}

func BenchmarkSortedListRemove(b *testing.B) {
	l := NewSortedList()

	var mockItems []*item
	for i := 0; i < b.N; i++ {
		mockItems = append(mockItems, &item{id: i})
	}

	for _, mockItem := range mockItems {
		l.Insert(mockItem)
	}

	b.ResetTimer()
	for _, mockItem := range mockItems {
		l.Remove(mockItem)
	}
}

func BenchmarkSortedListContains(b *testing.B) {
	l := NewSortedList()

	var mockItems []*item
	for i := 0; i < b.N; i++ {
		mockItems = append(mockItems, &item{id: i})
	}

	for _, mockItem := range mockItems {
		l.Insert(mockItem)
	}

	b.ResetTimer()
	for _, mockItem := range mockItems {
		l.Contains(mockItem)
	}
}

func BenchmarkSortedListRange(b *testing.B) {
	l := NewSortedList()

	var mockItems []*item
	for i := 0; i < b.N; i++ {
		mockItems = append(mockItems, &item{id: i})
	}

	for _, mockItem := range mockItems {
		l.Insert(mockItem)
	}

	b.ResetTimer()
	l.Range(func(_ Item) bool { return true })
}

func BenchmarkSortedListReverseRange(b *testing.B) {
	l := NewSortedList()

	var mockItems []*item
	for i := 0; i < b.N; i++ {
		mockItems = append(mockItems, &item{id: i})
	}

	for _, mockItem := range mockItems {
		l.Insert(mockItem)
	}

	b.ResetTimer()
	l.ReverseRange(func(_ Item) bool { return true })
}
