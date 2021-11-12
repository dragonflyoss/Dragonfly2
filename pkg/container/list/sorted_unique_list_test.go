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

	"d7y.io/dragonfly/v2/pkg/container/list/mocks"
)

func TestSortedUniqueListInsert(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(mocks ...*mocks.MockItemMockRecorder)
		expect func(t *testing.T, ul SortedUniqueList, items ...Item)
	}{
		{
			name: "insert values succeeded",
			mock: func(m ...*mocks.MockItemMockRecorder) {},
			expect: func(t *testing.T, ul SortedUniqueList, items ...Item) {
				assert := assert.New(t)
				ul.Insert(items[0])
				assert.Equal(ul.Contains(items[0]), true)
				assert.Equal(ul.Len(), 1)
			},
		},
		{
			name: "insert multi value succeeded",
			mock: func(m ...*mocks.MockItemMockRecorder) {
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
			mock: func(m ...*mocks.MockItemMockRecorder) {},
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

			mockItems := []*mocks.MockItem{mocks.NewMockItem(ctl), mocks.NewMockItem(ctl)}
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
		mockItem := mocks.NewMockItem(ctl)
		mockItem.EXPECT().SortedValue().DoAndReturn(func() uint { return uint(v) }).AnyTimes()
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
		mock   func(mocks ...*mocks.MockItemMockRecorder)
		expect func(t *testing.T, ul SortedUniqueList, items ...Item)
	}{
		{
			name: "remove values succeeded",
			mock: func(m ...*mocks.MockItemMockRecorder) {
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
			mock: func(m ...*mocks.MockItemMockRecorder) {},
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

			mockItems := []*mocks.MockItem{mocks.NewMockItem(ctl), mocks.NewMockItem(ctl)}
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
		mockItem := mocks.NewMockItem(ctl)
		mockItem.EXPECT().SortedValue().DoAndReturn(func() uint { return uint(v) }).AnyTimes()
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

// func TestSortedUniqueListContains(t *testing.T) {
// tests := []struct {
// name   string
// mock   func(mocks ...*mocks.MockItemMockRecorder)
// expect func(t *testing.T, l SortedList, items ...Item)
// }{
// {
// name: "contains values succeeded",
// mock: func(m ...*mocks.MockItemMockRecorder) {
// gomock.InOrder(
// m[0].SortedValue().Return(0).Times(1),
// m[1].SortedValue().Return(1).Times(1),
// )
// },
// expect: func(t *testing.T, l SortedList, items ...Item) {
// assert := assert.New(t)
// l.Insert(items[0])
// l.Insert(items[1])
// assert.Equal(l.Contains(items[0]), true)
// assert.Equal(l.Contains(items[1]), true)
// assert.Equal(l.Len(), 2)
// },
// },
// {
// name: "contains value dost not exits",
// mock: func(m ...*mocks.MockItemMockRecorder) {},
// expect: func(t *testing.T, l SortedList, items ...Item) {
// assert := assert.New(t)
// l.Insert(items[0])
// assert.Equal(l.Contains(items[1]), false)
// },
// },
// }

// for _, tc := range tests {
// t.Run(tc.name, func(t *testing.T) {
// ctl := gomock.NewController(t)
// defer ctl.Finish()

// mockItems := []*mocks.MockItem{mocks.NewMockItem(ctl), mocks.NewMockItem(ctl)}
// tc.mock(mockItems[0].EXPECT(), mockItems[1].EXPECT())
// tc.expect(t, NewSortedList(), mockItems[0], mockItems[1])
// })
// }
// }

// func TestSortedUniqueListContains_Concurrent(t *testing.T) {
// runtime.GOMAXPROCS(2)

// ctl := gomock.NewController(t)
// defer ctl.Finish()
// mockItem := mocks.NewMockItem(ctl)
// mockItem.EXPECT().SortedValue().DoAndReturn(func() uint { return uint(rand.Intn(N)) }).AnyTimes()

// l := NewSortedList()
// nums := rand.Perm(N)
// for range nums {
// l.Insert(mockItem)
// }

// var wg sync.WaitGroup
// for range nums {
// wg.Add(1)
// go func() {
// if ok := l.Contains(mockItem); !ok {
// t.Error("SortedList contains error")
// }
// wg.Done()
// }()
// }
// wg.Wait()
// }

// func TestSortedUniqueListLen(t *testing.T) {
// tests := []struct {
// name   string
// mock   func(mocks ...*mocks.MockItemMockRecorder)
// expect func(t *testing.T, l SortedList, items ...Item)
// }{
// {
// name: "get length succeeded",
// mock: func(m ...*mocks.MockItemMockRecorder) {
// gomock.InOrder(
// m[0].SortedValue().Return(0).Times(1),
// m[1].SortedValue().Return(1).Times(1),
// )
// },
// expect: func(t *testing.T, l SortedList, items ...Item) {
// assert := assert.New(t)
// l.Insert(items[0])
// l.Insert(items[1])
// assert.Equal(l.Len(), 2)
// },
// },
// {
// name: "get empty list length",
// mock: func(m ...*mocks.MockItemMockRecorder) {},
// expect: func(t *testing.T, l SortedList, items ...Item) {
// assert := assert.New(t)
// assert.Equal(l.Len(), 0)
// },
// },
// }

// for _, tc := range tests {
// t.Run(tc.name, func(t *testing.T) {
// ctl := gomock.NewController(t)
// defer ctl.Finish()

// mockItems := []*mocks.MockItem{mocks.NewMockItem(ctl), mocks.NewMockItem(ctl)}
// tc.mock(mockItems[0].EXPECT(), mockItems[1].EXPECT())
// tc.expect(t, NewSortedList(), mockItems[0], mockItems[1])
// })
// }
// }

// func TestSortedUniqueListLen_Concurrent(t *testing.T) {
// runtime.GOMAXPROCS(2)

// ctl := gomock.NewController(t)
// defer ctl.Finish()
// mockItem := mocks.NewMockItem(ctl)
// mockItem.EXPECT().SortedValue().DoAndReturn(func() uint { return uint(rand.Intn(N)) }).AnyTimes()

// l := NewSortedList()

// var wg sync.WaitGroup
// wg.Add(1)
// go func() {
// elems := l.Len()
// for i := 0; i < N; i++ {
// newElems := l.Len()
// if newElems < elems {
// t.Errorf("Len shrunk from %v to %v", elems, newElems)
// }
// }
// wg.Done()
// }()

// for i := 0; i < N; i++ {
// l.Insert(mockItem)
// }
// wg.Wait()
// }

// func TestSortedUniqueListRange(t *testing.T) {
// tests := []struct {
// name   string
// mock   func(mocks ...*mocks.MockItemMockRecorder)
// expect func(t *testing.T, l SortedList, items ...Item)
// }{
// {
// name: "range succeeded",
// mock: func(m ...*mocks.MockItemMockRecorder) {
// gomock.InOrder(
// m[0].SortedValue().Return(0).Times(1),
// m[1].SortedValue().Return(1).Times(1),
// )
// },
// expect: func(t *testing.T, l SortedList, items ...Item) {
// assert := assert.New(t)
// l.Insert(items[0])
// l.Insert(items[1])
// assert.Equal(l.Len(), 2)

// i := 0
// l.Range(func(item Item) bool {
// if i == 0 {
// assert.Equal(item, items[0])
// } else {
// assert.Equal(item, items[1])
// }
// return true
// })
// },
// },
// {
// name: "range stoped",
// mock: func(m ...*mocks.MockItemMockRecorder) {
// gomock.InOrder(
// m[0].SortedValue().Return(0).Times(1),
// m[1].SortedValue().Return(1).Times(1),
// )
// },
// expect: func(t *testing.T, l SortedList, items ...Item) {
// assert := assert.New(t)
// l.Insert(items[0])
// l.Insert(items[1])
// assert.Equal(l.Len(), 2)

// l.Range(func(item Item) bool {
// assert.Equal(item, items[0])
// return false
// })
// },
// },
// {
// name: "range same values",
// mock: func(m ...*mocks.MockItemMockRecorder) {
// gomock.InOrder(
// m[0].SortedValue().Return(0).AnyTimes(),
// )
// },
// expect: func(t *testing.T, l SortedList, items ...Item) {
// assert := assert.New(t)
// l.Insert(items[0])
// l.Insert(items[0])
// l.Insert(items[0])
// assert.Equal(l.Len(), 3)

// count := 0
// l.Range(func(item Item) bool {
// assert.Equal(item, items[0])
// count++
// return true
// })
// assert.Equal(count, 3)
// },
// },
// {
// name: "range empty list",
// mock: func(m ...*mocks.MockItemMockRecorder) {
// },
// expect: func(t *testing.T, l SortedList, items ...Item) {
// assert := assert.New(t)
// count := 0
// l.Range(func(item Item) bool {
// count++
// return true
// })
// assert.Equal(count, 0)
// },
// },
// }

// for _, tc := range tests {
// t.Run(tc.name, func(t *testing.T) {
// ctl := gomock.NewController(t)
// defer ctl.Finish()

// mockItems := []*mocks.MockItem{mocks.NewMockItem(ctl), mocks.NewMockItem(ctl)}
// tc.mock(mockItems[0].EXPECT(), mockItems[1].EXPECT())
// tc.expect(t, NewSortedList(), mockItems[0], mockItems[1])
// })
// }
// }

// func TestSortedUniqueListRange_Concurrent(t *testing.T) {
// runtime.GOMAXPROCS(2)

// ctl := gomock.NewController(t)
// defer ctl.Finish()
// mockItem := mocks.NewMockItem(ctl)
// mockItem.EXPECT().SortedValue().DoAndReturn(func() uint { return uint(rand.Intn(N)) }).AnyTimes()

// l := NewSortedList()
// var wg sync.WaitGroup
// wg.Add(1)
// go func() {
// i := 0
// l.Range(func(_ Item) bool {
// i++
// return true
// })

// j := 0
// l.Range(func(_ Item) bool {
// j++
// return true
// })
// if j < i {
// t.Errorf("Values shrunk from %v to %v", i, j)
// }
// wg.Done()
// }()

// for i := 0; i < N; i++ {
// l.Insert(mockItem)
// }
// wg.Wait()
// }

// func TestSortedUniqueListReverseRange(t *testing.T) {
// tests := []struct {
// name   string
// mock   func(mocks ...*mocks.MockItemMockRecorder)
// expect func(t *testing.T, l SortedList, items ...Item)
// }{
// {
// name: "reverse range succeeded",
// mock: func(m ...*mocks.MockItemMockRecorder) {
// gomock.InOrder(
// m[0].SortedValue().Return(0).Times(1),
// m[1].SortedValue().Return(1).Times(1),
// )
// },
// expect: func(t *testing.T, l SortedList, items ...Item) {
// assert := assert.New(t)
// l.Insert(items[0])
// l.Insert(items[1])
// assert.Equal(l.Len(), 2)

// i := 0
// l.ReverseRange(func(item Item) bool {
// if i == 0 {
// assert.Equal(item, items[1])
// } else {
// assert.Equal(item, items[0])
// }
// return true
// })
// },
// },
// {
// name: "reverse range stoped",
// mock: func(m ...*mocks.MockItemMockRecorder) {
// gomock.InOrder(
// m[0].SortedValue().Return(0).Times(1),
// m[1].SortedValue().Return(1).Times(1),
// )
// },
// expect: func(t *testing.T, l SortedList, items ...Item) {
// assert := assert.New(t)
// l.Insert(items[0])
// l.Insert(items[1])
// assert.Equal(l.Len(), 2)

// l.ReverseRange(func(item Item) bool {
// assert.Equal(item, items[1])
// return false
// })
// },
// },
// {
// name: "reverse range same values",
// mock: func(m ...*mocks.MockItemMockRecorder) {
// gomock.InOrder(
// m[0].SortedValue().Return(0).AnyTimes(),
// )
// },
// expect: func(t *testing.T, l SortedList, items ...Item) {
// assert := assert.New(t)
// l.Insert(items[0])
// l.Insert(items[0])
// l.Insert(items[0])
// assert.Equal(l.Len(), 3)

// count := 0
// l.ReverseRange(func(item Item) bool {
// assert.Equal(item, items[0])
// count++
// return true
// })
// assert.Equal(count, 3)
// },
// },
// {
// name: "reverse range empty list",
// mock: func(m ...*mocks.MockItemMockRecorder) {
// },
// expect: func(t *testing.T, l SortedList, items ...Item) {
// assert := assert.New(t)
// count := 0
// l.ReverseRange(func(item Item) bool {
// count++
// return true
// })
// assert.Equal(count, 0)
// },
// },
// }

// for _, tc := range tests {
// t.Run(tc.name, func(t *testing.T) {
// ctl := gomock.NewController(t)
// defer ctl.Finish()

// mockItems := []*mocks.MockItem{mocks.NewMockItem(ctl), mocks.NewMockItem(ctl)}
// tc.mock(mockItems[0].EXPECT(), mockItems[1].EXPECT())
// tc.expect(t, NewSortedList(), mockItems[0], mockItems[1])
// })
// }
// }

// func TestSortedUniqueListReverseRange_Concurrent(t *testing.T) {
// runtime.GOMAXPROCS(2)

// ctl := gomock.NewController(t)
// defer ctl.Finish()
// mockItem := mocks.NewMockItem(ctl)
// mockItem.EXPECT().SortedValue().DoAndReturn(func() uint { return uint(rand.Intn(N)) }).AnyTimes()

// l := NewSortedList()
// var wg sync.WaitGroup
// wg.Add(1)
// go func() {
// i := 0
// l.ReverseRange(func(_ Item) bool {
// i++
// return true
// })

// j := 0
// l.ReverseRange(func(_ Item) bool {
// j++
// return true
// })
// if j < i {
// t.Errorf("Values shrunk from %v to %v", i, j)
// }
// wg.Done()
// }()

// for i := 0; i < N; i++ {
// l.Insert(mockItem)
// }
// wg.Wait()
// }

// func BenchmarkSortedUniqueListAdd(b *testing.B) {
// l := NewSortedList()
// mockItem := &item{}

// b.ResetTimer()
// for i := 0; i < b.N; i++ {
// l.Insert(mockItem)
// }
// }

// func BenchmarkSortedUniqueListRemove(b *testing.B) {
// l := NewSortedList()
// mockItem := &item{}

// for i := 0; i < b.N; i++ {
// l.Insert(mockItem)
// }

// b.ResetTimer()
// for i := 0; i < b.N; i++ {
// l.Remove(mockItem)
// }
// }

// func BenchmarkSortedUniqueListContains(b *testing.B) {
// l := NewSortedList()
// mockItem := &item{}

// for i := 0; i < b.N; i++ {
// l.Insert(mockItem)
// }

// b.ResetTimer()
// for i := 0; i < b.N; i++ {
// l.Contains(mockItem)
// }
// }

// func BenchmarkSortedUniqueListRange(b *testing.B) {
// l := NewSortedList()
// mockItem := &item{}

// for i := 0; i < b.N; i++ {
// l.Insert(mockItem)
// }

// b.ResetTimer()
// l.Range(func(_ Item) bool { return true })
// }

// func BenchmarkSortedUniqueListReverseRange(b *testing.B) {
// l := NewSortedList()
// mockItem := &item{}

// for i := 0; i < b.N; i++ {
// l.Insert(mockItem)
// }

// b.ResetTimer()
// l.ReverseRange(func(_ Item) bool { return true })
// }
