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

func TestSortedUniqueListInsert(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(m ...*MockItemMockRecorder)
		expect func(t *testing.T, ul SortedUniqueList, items ...Item)
	}{
		{
			name: "insert values",
			mock: func(m ...*MockItemMockRecorder) {},
			expect: func(t *testing.T, ul SortedUniqueList, items ...Item) {
				assert := assert.New(t)
				ul.Insert(items[0])
				assert.Equal(ul.Contains(items[0]), true)
				assert.Equal(ul.Len(), 1)
			},
		},
		{
			name: "insert multi values",
			mock: func(m ...*MockItemMockRecorder) {
				gomock.InOrder(
					m[0].SortedValue().Return(0).Times(1),
					m[1].SortedValue().Return(1).Times(1),
				)
			},
			expect: func(t *testing.T, ul SortedUniqueList, items ...Item) {
				assert := assert.New(t)
				ul.Insert(items[0])
				ul.Insert(items[1])
				assert.Equal(ul.Contains(items[0]), true)
				assert.Equal(ul.Contains(items[1]), true)
				assert.Equal(ul.Len(), 2)
			},
		},
		{
			name: "insert same values",
			mock: func(m ...*MockItemMockRecorder) {},
			expect: func(t *testing.T, ul SortedUniqueList, items ...Item) {
				assert := assert.New(t)
				ul.Insert(items[0])
				ul.Insert(items[0])
				assert.Equal(ul.Contains(items[0]), true)
				assert.Equal(ul.Len(), 1)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			mockItems := []*MockItem{NewMockItem(ctl), NewMockItem(ctl)}
			tc.mock(mockItems[0].EXPECT(), mockItems[1].EXPECT())
			tc.expect(t, NewSortedUniqueList(), mockItems[0], mockItems[1])
		})
	}
}

func TestSortedUniqueListInsert_Concurrent(t *testing.T) {
	runtime.GOMAXPROCS(2)

	ctl := gomock.NewController(t)
	defer ctl.Finish()

	ul := NewSortedUniqueList()
	nums := rand.Perm(N)

	var mockItems []Item
	for _, v := range nums {
		mockItem := NewMockItem(ctl)
		mockItem.EXPECT().SortedValue().DoAndReturn(func() int { return v }).AnyTimes()
		mockItems = append(mockItems, mockItem)
	}

	var wg sync.WaitGroup
	wg.Add(len(mockItems))
	for _, mockItem := range mockItems {
		go func(item Item) {
			ul.Insert(item)
			wg.Done()
		}(mockItem)
	}

	wg.Wait()
	count := 0
	ul.Range(func(item Item) bool {
		count++
		return true
	})
	if count != len(nums) {
		t.Errorf("SortedUniqueList is missing element")
	}
}

func TestSortedUniqueListRemove(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(m ...*MockItemMockRecorder)
		expect func(t *testing.T, ul SortedUniqueList, items ...Item)
	}{
		{
			name: "remove values",
			mock: func(m ...*MockItemMockRecorder) {
				gomock.InOrder(
					m[0].SortedValue().Return(0).Times(1),
					m[1].SortedValue().Return(1).Times(1),
				)
			},
			expect: func(t *testing.T, ul SortedUniqueList, items ...Item) {
				assert := assert.New(t)
				ul.Insert(items[0])
				ul.Insert(items[1])
				assert.Equal(ul.Contains(items[0]), true)
				assert.Equal(ul.Contains(items[1]), true)
				assert.Equal(ul.Len(), 2)
				ul.Remove(items[0])
				assert.Equal(ul.Contains(items[0]), false)
				assert.Equal(ul.Len(), 1)
				ul.Remove(items[1])
				assert.Equal(ul.Contains(items[1]), false)
				assert.Equal(ul.Len(), 0)
			},
		},
		{
			name: "remove value dost not exits",
			mock: func(m ...*MockItemMockRecorder) {},
			expect: func(t *testing.T, ul SortedUniqueList, items ...Item) {
				assert := assert.New(t)
				ul.Insert(items[0])
				assert.Equal(ul.Contains(items[0]), true)
				assert.Equal(ul.Len(), 1)
				ul.Remove(items[1])
				assert.Equal(ul.Contains(items[0]), true)
				assert.Equal(ul.Len(), 1)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			mockItems := []*MockItem{NewMockItem(ctl), NewMockItem(ctl)}
			tc.mock(mockItems[0].EXPECT(), mockItems[1].EXPECT())
			tc.expect(t, NewSortedUniqueList(), mockItems[0], mockItems[1])
		})
	}
}

func TestSortedUniqueListRemove_Concurrent(t *testing.T) {
	runtime.GOMAXPROCS(2)

	ctl := gomock.NewController(t)
	defer ctl.Finish()

	ul := NewSortedUniqueList()
	nums := rand.Perm(N)

	var mockItems []Item
	for _, v := range nums {
		mockItem := NewMockItem(ctl)
		mockItem.EXPECT().SortedValue().DoAndReturn(func() int { return v }).AnyTimes()
		mockItems = append(mockItems, mockItem)
		ul.Insert(mockItem)
	}

	var wg sync.WaitGroup
	wg.Add(len(mockItems))
	for _, mockItem := range mockItems {
		go func(item Item) {
			ul.Remove(item)
			wg.Done()
		}(mockItem)
	}

	wg.Wait()
	if ul.Len() != 0 {
		t.Errorf("SortedUniqueList is redundant elements")
	}
}

func TestSortedUniqueListContains(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(m ...*MockItemMockRecorder)
		expect func(t *testing.T, ul SortedUniqueList, items ...Item)
	}{
		{
			name: "contains values",
			mock: func(m ...*MockItemMockRecorder) {
				gomock.InOrder(
					m[0].SortedValue().Return(0).Times(1),
					m[1].SortedValue().Return(1).Times(1),
				)
			},
			expect: func(t *testing.T, ul SortedUniqueList, items ...Item) {
				assert := assert.New(t)
				ul.Insert(items[0])
				ul.Insert(items[1])
				assert.Equal(ul.Contains(items[0]), true)
				assert.Equal(ul.Contains(items[1]), true)
			},
		},
		{
			name: "contains value dost not exits",
			mock: func(m ...*MockItemMockRecorder) {},
			expect: func(t *testing.T, ul SortedUniqueList, items ...Item) {
				assert := assert.New(t)
				ul.Insert(items[0])
				assert.Equal(ul.Contains(items[1]), false)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			mockItems := []*MockItem{NewMockItem(ctl), NewMockItem(ctl)}
			tc.mock(mockItems[0].EXPECT(), mockItems[1].EXPECT())
			tc.expect(t, NewSortedUniqueList(), mockItems[0], mockItems[1])
		})
	}
}

func TestSortedUniqueListContains_Concurrent(t *testing.T) {
	runtime.GOMAXPROCS(2)

	ctl := gomock.NewController(t)
	defer ctl.Finish()

	ul := NewSortedUniqueList()
	nums := rand.Perm(N)

	var mockItems []Item
	for _, v := range nums {
		mockItem := NewMockItem(ctl)
		mockItem.EXPECT().SortedValue().DoAndReturn(func() int { return v }).AnyTimes()
		mockItems = append(mockItems, mockItem)
		ul.Insert(mockItem)
	}

	var wg sync.WaitGroup
	wg.Add(len(mockItems))
	for _, mockItem := range mockItems {
		go func(item Item) {
			if ok := ul.Contains(item); !ok {
				t.Error("SortedUniqueList contains error")
			}
			wg.Done()
		}(mockItem)
	}
	wg.Wait()
}

func TestSortedUniqueListLen(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(m ...*MockItemMockRecorder)
		expect func(t *testing.T, ul SortedUniqueList, items ...Item)
	}{
		{
			name: "get length",
			mock: func(m ...*MockItemMockRecorder) {
				gomock.InOrder(
					m[0].SortedValue().Return(0).Times(1),
					m[1].SortedValue().Return(1).Times(1),
				)
			},
			expect: func(t *testing.T, ul SortedUniqueList, items ...Item) {
				assert := assert.New(t)
				ul.Insert(items[0])
				ul.Insert(items[1])
				assert.Equal(ul.Len(), 2)
			},
		},
		{
			name: "get empty list length",
			mock: func(m ...*MockItemMockRecorder) {},
			expect: func(t *testing.T, ul SortedUniqueList, items ...Item) {
				assert := assert.New(t)
				assert.Equal(ul.Len(), 0)
			},
		},
		{
			name: "get same values length",
			mock: func(m ...*MockItemMockRecorder) {},
			expect: func(t *testing.T, ul SortedUniqueList, items ...Item) {
				assert := assert.New(t)
				ul.Insert(items[0])
				ul.Insert(items[0])
				assert.Equal(ul.Len(), 1)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			mockItems := []*MockItem{NewMockItem(ctl), NewMockItem(ctl)}
			tc.mock(mockItems[0].EXPECT(), mockItems[1].EXPECT())
			tc.expect(t, NewSortedUniqueList(), mockItems[0], mockItems[1])
		})
	}
}

func TestSortedUniqueListLen_Concurrent(t *testing.T) {
	runtime.GOMAXPROCS(2)

	ctl := gomock.NewController(t)
	defer ctl.Finish()

	ul := NewSortedUniqueList()
	nums := rand.Perm(N)

	var mockItems []Item
	for _, v := range nums {
		mockItem := NewMockItem(ctl)
		mockItem.EXPECT().SortedValue().DoAndReturn(func() int { return v }).AnyTimes()
		mockItems = append(mockItems, mockItem)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		elems := ul.Len()
		for i := 0; i < N; i++ {
			newElems := ul.Len()
			if newElems < elems {
				t.Errorf("Len shrunk from %v to %v", elems, newElems)
			}
		}
		wg.Done()
	}()

	for _, mockItem := range mockItems {
		ul.Insert(mockItem)
	}
	wg.Wait()
}

func TestSortedUniqueListRange(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(m ...*MockItemMockRecorder)
		expect func(t *testing.T, ul SortedUniqueList, items ...Item)
	}{
		{
			name: "range values",
			mock: func(m ...*MockItemMockRecorder) {
				gomock.InOrder(
					m[0].SortedValue().Return(0).Times(1),
					m[1].SortedValue().Return(1).Times(1),
				)
			},
			expect: func(t *testing.T, ul SortedUniqueList, items ...Item) {
				assert := assert.New(t)
				ul.Insert(items[0])
				ul.Insert(items[1])
				assert.Equal(ul.Len(), 2)

				i := 0
				ul.Range(func(item Item) bool {
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
			expect: func(t *testing.T, ul SortedUniqueList, items ...Item) {
				assert := assert.New(t)
				for _, item := range items {
					ul.Insert(item)
				}
				ul.Insert(items[1])
				assert.Equal(ul.Len(), 10)

				i := 0
				ul.Range(func(item Item) bool {
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
			expect: func(t *testing.T, ul SortedUniqueList, items ...Item) {
				assert := assert.New(t)
				ul.Insert(items[0])
				ul.Insert(items[1])
				assert.Equal(ul.Len(), 2)

				ul.Range(func(item Item) bool {
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
			expect: func(t *testing.T, ul SortedUniqueList, items ...Item) {
				assert := assert.New(t)
				ul.Insert(items[0])
				ul.Insert(items[0])
				ul.Insert(items[0])
				assert.Equal(ul.Len(), 1)

				count := 0
				ul.Range(func(item Item) bool {
					assert.Equal(item, items[0])
					count++
					return true
				})
				assert.Equal(count, 1)
			},
		},
		{
			name: "range empty list",
			mock: func(m ...*MockItemMockRecorder) {
			},
			expect: func(t *testing.T, ul SortedUniqueList, items ...Item) {
				assert := assert.New(t)
				count := 0
				ul.Range(func(item Item) bool {
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
			tc.expect(t, NewSortedUniqueList(), mockItems...)
		})
	}
}

func TestSortedUniqueListRange_Concurrent(t *testing.T) {
	runtime.GOMAXPROCS(2)

	ctl := gomock.NewController(t)
	defer ctl.Finish()

	ul := NewSortedUniqueList()
	nums := rand.Perm(N)

	var mockItems []Item
	for _, v := range nums {
		mockItem := NewMockItem(ctl)
		mockItem.EXPECT().SortedValue().DoAndReturn(func() int { return v }).AnyTimes()
		mockItems = append(mockItems, mockItem)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		i := 0
		ul.Range(func(_ Item) bool {
			i++
			return true
		})

		j := 0
		ul.Range(func(_ Item) bool {
			j++
			return true
		})
		if j < i {
			t.Errorf("Values shrunk from %v to %v", i, j)
		}
		wg.Done()
	}()

	for _, mockItem := range mockItems {
		ul.Insert(mockItem)
	}
	wg.Wait()
}

func TestSortedUniqueListReverseRange(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(m ...*MockItemMockRecorder)
		expect func(t *testing.T, ul SortedUniqueList, items ...Item)
	}{
		{
			name: "reverse range values",
			mock: func(m ...*MockItemMockRecorder) {
				gomock.InOrder(
					m[0].SortedValue().Return(0).Times(1),
					m[1].SortedValue().Return(1).Times(1),
				)
			},
			expect: func(t *testing.T, ul SortedUniqueList, items ...Item) {
				assert := assert.New(t)
				ul.Insert(items[0])
				ul.Insert(items[1])
				assert.Equal(ul.Len(), 2)

				i := 1
				ul.ReverseRange(func(item Item) bool {
					assert.Equal(item, items[i])
					i--
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
			expect: func(t *testing.T, ul SortedUniqueList, items ...Item) {
				assert := assert.New(t)
				for _, item := range items {
					ul.Insert(item)
				}
				ul.Insert(items[1])
				assert.Equal(ul.Len(), 10)

				i := 9
				ul.Range(func(item Item) bool {
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
			expect: func(t *testing.T, ul SortedUniqueList, items ...Item) {
				assert := assert.New(t)
				ul.Insert(items[0])
				ul.Insert(items[1])
				assert.Equal(ul.Len(), 2)

				ul.ReverseRange(func(item Item) bool {
					assert.Equal(item, items[0])
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
			expect: func(t *testing.T, ul SortedUniqueList, items ...Item) {
				assert := assert.New(t)
				ul.Insert(items[0])
				ul.Insert(items[0])
				ul.Insert(items[0])
				assert.Equal(ul.Len(), 1)

				count := 0
				ul.ReverseRange(func(item Item) bool {
					assert.Equal(item, items[0])
					count++
					return true
				})
				assert.Equal(count, 1)
			},
		},
		{
			name: "reverse range empty list",
			mock: func(m ...*MockItemMockRecorder) {
			},
			expect: func(t *testing.T, ul SortedUniqueList, items ...Item) {
				assert := assert.New(t)
				count := 0
				ul.ReverseRange(func(item Item) bool {
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
			tc.expect(t, NewSortedUniqueList(), mockItems...)
		})
	}
}

func TestSortedUniqueListReverseRange_Concurrent(t *testing.T) {
	runtime.GOMAXPROCS(2)

	ctl := gomock.NewController(t)
	defer ctl.Finish()

	ul := NewSortedUniqueList()
	nums := rand.Perm(N)

	var mockItems []Item
	for _, v := range nums {
		mockItem := NewMockItem(ctl)
		mockItem.EXPECT().SortedValue().DoAndReturn(func() int { return v }).AnyTimes()
		mockItems = append(mockItems, mockItem)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		i := 0
		ul.ReverseRange(func(_ Item) bool {
			i++
			return true
		})

		j := 0
		ul.ReverseRange(func(_ Item) bool {
			j++
			return true
		})
		if j < i {
			t.Errorf("Values shrunk from %v to %v", i, j)
		}
		wg.Done()
	}()

	for _, mockItem := range mockItems {
		ul.Insert(mockItem)
	}
	wg.Wait()
}

func BenchmarkSortedUniqueListInsert(b *testing.B) {
	ul := NewSortedUniqueList()

	var mockItems []*item
	for i := 0; i < b.N; i++ {
		mockItems = append(mockItems, &item{id: i})
	}

	b.ResetTimer()
	for _, mockItem := range mockItems {
		ul.Insert(mockItem)
	}
}

func BenchmarkSortedUniqueListRemove(b *testing.B) {
	ul := NewSortedUniqueList()

	var mockItems []*item
	for i := 0; i < b.N; i++ {
		mockItem := &item{id: i}
		ul.Insert(mockItem)
		mockItems = append(mockItems, mockItem)
	}

	b.ResetTimer()
	for _, mockItem := range mockItems {
		ul.Remove(mockItem)
	}
}

func BenchmarkSortedUniqueListContains(b *testing.B) {
	ul := NewSortedUniqueList()

	var mockItems []*item
	for i := 0; i < b.N; i++ {
		mockItem := &item{id: i}
		ul.Insert(mockItem)
		mockItems = append(mockItems, mockItem)
	}

	b.ResetTimer()
	for _, mockItem := range mockItems {
		ul.Contains(mockItem)
	}
}

func BenchmarkSortedUniqueListRange(b *testing.B) {
	ul := NewSortedUniqueList()

	for i := 0; i < b.N; i++ {
		mockItem := item{id: i}
		ul.Insert(&mockItem)
	}

	b.ResetTimer()
	ul.Range(func(_ Item) bool { return true })
}

func BenchmarkSortedUniqueListReverseRange(b *testing.B) {
	ul := NewSortedUniqueList()

	for i := 0; i < b.N; i++ {
		mockItem := item{id: i}
		ul.Insert(&mockItem)
	}

	b.ResetTimer()
	ul.ReverseRange(func(item Item) bool { return true })
}
