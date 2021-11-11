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

package sortedlist

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"

	"d7y.io/dragonfly/v2/pkg/structure/sortedlist/mocks"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

type item struct {
	key1 int
	key2 int
}

func NewSortedListItem(key1, key2 int) *item {
	return &item{key1, key2}
}

func (i *item) GetSortKeys() (int, int) {
	return i.key1, i.key2
}

func TestAdd(t *testing.T) {
	l := NewSortedListSortedList()
	l.Add(NewSortedListItem(1, 2))
	l.Add(NewSortedListItem(2, 2))
	l.Add(NewSortedListItem(2, 3))
	if l.Size() != 3 {
		t.Errorf("TestAdd failed count required[3] but get [%d]", l.Size())
	}
}

func TestDelete(t *testing.T) {
	l := NewSortedListSortedList()
	it := NewSortedListItem(1, 3)
	l.Add(NewSortedListItem(1, 2))
	l.Add(NewSortedListItem(2, 2))
	l.Add(it)
	l.Add(NewSortedListItem(2, 3))
	l.Delete(it)
	if l.Size() != 3 {
		t.Errorf("TestDelete failed count required[3] but get [%d]", l.Size())
	}
}

func TestUpdate(t *testing.T) {
	l := &sortedList{
		left:   0,
		right:  0,
		keyMap: make(map[Item]int),
	}
	it := NewSortedListItem(1, 3)
	l.Add(NewSortedListItem(1, 2))
	l.Add(NewSortedListItem(2, 2))
	l.Add(it)
	it.key1 = 2
	l.Update(it)
	l.Add(NewSortedListItem(2, 3))
	key1, key2, ok := l.getKeyMapKey(it)
	if l.Size() != 4 || key1 != 2 || key2 != 3 || !ok {
		t.Errorf("TestUpdate failed count required[3] but get [%d]", l.Size())
	}
}

func TestRange(t *testing.T) {
	l := &sortedList{
		left:   0,
		right:  0,
		keyMap: make(map[Item]int),
	}
	it := NewSortedListItem(1, 3)
	l.Add(NewSortedListItem(1, 2))
	l.Add(NewSortedListItem(2, 2))
	l.Add(it)
	it.key1 = 2
	l.Update(it)
	l.Add(NewSortedListItem(2, 3))
	key1, key2, ok := l.getKeyMapKey(it)
	if l.Size() != 4 || key1 != 2 || key2 != 3 || !ok {
		t.Errorf("TestUpdate failed count required[4] but get [%d]", l.Size())
	}

	count := 0
	l.Range(func(data Item) bool {
		it := data.(*item)
		fmt.Println(it.key1, it.key2)
		count++
		return true
	})
	if l.Size() != count {
		t.Errorf("TestRange failed count required[4] but get [%d]", l.Size())
	}
}

func TestRangeLimit(t *testing.T) {
	l := &sortedList{
		left:   0,
		right:  0,
		keyMap: make(map[Item]int),
	}
	it := NewSortedListItem(1, 3)
	l.Add(NewSortedListItem(1, 2))
	l.Add(NewSortedListItem(2, 2))
	l.Add(it)
	it.key1 = 2
	l.Update(it)
	l.Add(NewSortedListItem(2, 3))
	key1, key2, ok := l.getKeyMapKey(it)
	if l.Size() != 4 || key1 != 2 || key2 != 3 || !ok {
		t.Errorf("TestUpdate failed count required[4] but get [%d]", l.Size())
	}

	count := 0
	l.RangeLimit(2, func(data Item) bool {
		it := data.(*item)
		fmt.Println(it.key1, it.key2)
		count++
		return true
	})
	if 2 != count {
		t.Errorf("TestRange failed count required[2] but get [%d]", count)
	}
}

func TestRangeReverse(t *testing.T) {
	l := &sortedList{
		left:   0,
		right:  0,
		keyMap: make(map[Item]int),
	}
	it := NewSortedListItem(1, 3)
	l.Add(NewSortedListItem(1, 2))
	l.Add(NewSortedListItem(2, 2))
	l.Add(it)
	it.key1 = 2
	l.Update(it)
	l.Add(NewSortedListItem(2, 3))
	key1, key2, ok := l.getKeyMapKey(it)
	if l.Size() != 4 || key1 != 2 || key2 != 3 || !ok {
		t.Errorf("TestRangeReverse failed count required[4] but get [%d]", l.Size())
	}

	count := 0
	l.ReverseRange(func(data Item) bool {
		it := data.(*item)
		fmt.Println(it.key1, it.key2)
		count++
		return true
	})
	if l.Size() != count {
		t.Errorf("TestRangeReverse failed count required[4] but get [%d]", l.Size())
	}
}

func TestRangeReverseLimit(t *testing.T) {
	l := &sortedList{
		left:   0,
		right:  0,
		keyMap: make(map[Item]int),
	}
	it := NewSortedListItem(1, 3)
	l.Add(NewSortedListItem(1, 2))
	l.Add(NewSortedListItem(2, 2))
	l.Add(it)
	it.key1 = 2
	l.Update(it)
	l.Add(NewSortedListItem(2, 3))
	key1, key2, ok := l.getKeyMapKey(it)
	if l.Size() != 4 || key1 != 2 || key2 != 3 || !ok {
		t.Errorf("TestRangeReverseLimit failed count required[4] but get [%d]", l.Size())
	}

	count := 0
	l.ReverseRangeLimit(2, func(data Item) bool {
		it := data.(*item)
		fmt.Println(it.key1, it.key2)
		count++
		return true
	})
	if 2 != count {
		t.Errorf("TestRangeReverseLimit failed count required[4] but get [%d]", l.Size())
	}
}

func TestSortedListAdd(t *testing.T) {
	tests := []struct {
		name   string
		key    string
		len    uint
		mock   func(m *mocks.MockItemMockRecorder)
		expect func(t *testing.T, s SortedList, key string, item Item)
	}{
		{
			name: "add value succeeded",
			key:  "foo",
			len:  1,
			mock: func(m *mocks.MockItemMockRecorder) {
				m.GetSortKeys().Return(0).Times(2)
			},
			expect: func(t *testing.T, s sortedList, key string, item Item) {
				assert := assert.NewSortedList(t)
				assert.NoError(s.Add(item))
			},
		},
		{
			name: "item sorted value exceeds length",
			key:  "foo",
			len:  1,
			mock: func(m *mocks.MockItemMockRecorder) {
				m.GetSortKeys().Return(1).Times(1)
			},
			expect: func(t *testing.T, s SortedList, item Item) {
				assert := assert.NewSortedList(t)
				assert.EqualError(s.Add(item), "sorted value is illegal")
			},
		},
		{
			name: "add multi values",
			key:  "foo",
			len:  2,
			mock: func(m *mocks.MockItemMockRecorder) {
				m.GetSortKeys().Return(1).Times(4)
			},
			expect: func(t *testing.T, s SortedList, key string, item Item) {
				assert := assert.NewSortedList(t)
				assert.NoError(s.Add(key, item))
				assert.NoError(s.Add("bar", item))
			},
		},
		{
			name: "add same values",
			key:  "foo",
			len:  1,
			mock: func(m *mocks.MockItemMockRecorder) {
				m.GetSortKeys().Return(0).Times(5)
			},
			expect: func(t *testing.T, s SortedList, key string, item Item) {
				assert := assert.NewSortedList(t)
				assert.NoError(s.Add(key, item))
				assert.NoError(s.Add(key, item))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewSortedListController(t)
			defer ctl.Finish()
			mockItem := mocks.NewSortedListMockItem(ctl)
			tc.mock(mockItem.EXPECT())
			tc.expect(t, NewSortedList(tc.len), tc.key, mockItem)
		})
	}
}

func TestSortedListAdd_Concurrent(t *testing.T) {
	runtime.GOMAXPROCS(2)

	ctl := gomock.NewSortedListController(t)
	defer ctl.Finish()
	mockItem := mocks.NewSortedListMockItem(ctl)
	mockItem.EXPECT().GetSortKeys().DoAndReturn(func() uint { return rand.Intn(N)) }).AnyTimes()

	s := NewSortedListSortedList(N)
	nums := rand.Perm(N)

	var wg sync.WaitGroup
	wg.Add(len(nums))
	for i := 0; i < len(nums); i++ {
		go func(i int) {
			if err := s.Add(fmt.Sprint(i), mockItem); err != nil {
				t.Error(err)
			}
			wg.Done()
		}(i)
	}

	wg.Wait()
	count := 0
	s.Range(func(key string, item Item) bool {
		count++
		return true
	})
	if count != len(nums) {
		t.Errorf("SortedList is missing element")
	}
}

func TestSortedListUpdate(t *testing.T) {
	tests := []struct {
		name   string
		key    string
		len    uint
		mock   func(m *mocks.MockItemMockRecorder)
		expect func(t *testing.T, s SortedList, item Item)
	}{
		{
			name: "update value succeeded",
			key:  "foo",
			len:  1,
			mock: func(m *mocks.MockItemMockRecorder) {
				m.GetSortKeys().Return(0).Times(5)
			},
			expect: func(t *testing.T, s SortedList, item Item) {
				assert := assert.NewSortedList(t)
				assert.NoError(s.Update(item))
			},
		},
		{
			name: "item sorted value exceeds length",
			key:  "foo",
			len:  1,
			mock: func(m *mocks.MockItemMockRecorder) {
				gomock.InOrder(
					m.GetSortKeys().Return(0).Times(2),
					m.GetSortKeys().Return(1).Times(1),
				)
			},
			expect: func(t *testing.T, s SortedList, item Item) {
				assert := assert.NewSortedList(t)
				assert.EqualError(s.Add(item), "sorted value is illegal")
			},
		},
		{
			name: "add key dost not exits",
			key:  "foo",
			len:  1,
			mock: func(m *mocks.MockItemMockRecorder) {
				m.GetSortKeys().Return(0).Times(3)
			},
			expect: func(t *testing.T, s SortedList, item Item) {
				assert := assert.NewSortedList(t)
				assert.EqualError(s.Update(item), "key does not exist")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewSortedListController(t)
			defer ctl.Finish()
			mockItem := mocks.NewSortedListMockItem(ctl)
			tc.mock(mockItem.EXPECT())
			s := NewSortedList(tc.len)
			s.Add(tc.key, mockItem)
			tc.expect(t, s, tc.key, mockItem)
		})
	}
}

func TestSortedListUpdate_Concurrent(t *testing.T) {
	runtime.GOMAXPROCS(2)

	ctl := gomock.NewSortedListController(t)
	defer ctl.Finish()
	mockItem := mocks.NewSortedListMockItem(ctl)
	mockItem.EXPECT().GetSortKeys().AnyTimes()

	s := NewSortedListSortedList(N)
	nums := rand.Perm(N)

	for i := 0; i < len(nums); i++ {
		s.Add(fmt.Sprint(i), mockItem)
	}

	var wg sync.WaitGroup
	wg.Add(len(nums))
	for i := 0; i < len(nums); i++ {
		go func(v int) {
			if err := s.Update(fmt.Sprint(v), mockItem); err != nil {
				t.Error(err)
			}
			wg.Done()
		}(i)
	}

	wg.Wait()
	count := 0
	s.Range(func(item Item) bool {
		count++
		return true
	})
	if count != len(nums) {
		t.Errorf("SortedList is missing element")
	}
}

func TestSortedListDelete(t *testing.T) {
	tests := []struct {
		name   string
		key    string
		len    uint
		mock   func(m *mocks.MockItemMockRecorder)
		expect func(t *testing.T, s SortedList, item Item)
	}{
		{
			name: "delete value succeeded",
			key:  "foo",
			len:  1,
			mock: func(m *mocks.MockItemMockRecorder) {
				m.GetSortKeys().Return(0).Times(3)
			},
			expect: func(t *testing.T, s SortedList, item Item) {
				assert := assert.NewSortedList(t)
				assert.NoError(s.Delete(item))
				count := 0
				s.Range(func(item Item) bool {
					count++
					return true
				})

				assert.Equal(count, 0)
			},
		},
		{
			name: "delete key dost not exits",
			key:  "foo",
			len:  1,
			mock: func(m *mocks.MockItemMockRecorder) {
				m.GetSortKeys().Return(0).Times(2)
			},
			expect: func(t *testing.T, s SortedList, key string, item Item) {
				assert := assert.NewSortedList(t)
				assert.EqualError(s.Delete(item), "key does not exist")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewSortedListController(t)
			defer ctl.Finish()
			mockItem := mocks.NewSortedListMockItem(ctl)
			tc.mock(mockItem.EXPECT())
			s := NewSortedList(tc.len)
			s.Add(tc.key, mockItem)
			tc.expect(t, s, tc.key, mockItem)
		})
	}
}

func TestSortedListDelete_Concurrent(t *testing.T) {
	runtime.GOMAXPROCS(2)

	ctl := gomock.NewSortedListController(t)
	defer ctl.Finish()
	mockItem := mocks.NewSortedListMockItem(ctl)
	mockItem.EXPECT().GetSortKeys().DoAndReturn(func() uint { return rand.Intn(N) }).AnyTimes()

	s := NewSortedListSortedList(N)
	nums := rand.Perm(N)

	for i := 0; i < len(nums); i++ {
		s.Add(fmt.Sprint(i), mockItem)
	}

	var wg sync.WaitGroup
	wg.Add(len(nums))
	for i := 0; i < len(nums); i++ {
		go func(i int) {
			if err := s.Delete(fmt.Sprint(i)); err != nil {
				t.Error(err)
			}
			wg.Done()
		}(i)
	}

	wg.Wait()
	count := 0
	s.Range(func(item Item) bool {
		count++
		return true
	})
	if count != 0 {
		t.Errorf("SortedList is redundant elements")
	}
}

func TestSortedListRange(t *testing.T) {
	tests := []struct {
		name   string
		len    uint
		mock   func(m *mocks.MockItemMockRecorder)
		expect func(t *testing.T, s SortedList, item Item)
	}{
		{
			name: "range succeeded",
			len:  10,
			mock: func(m *mocks.MockItemMockRecorder) {
				m.GetSortKeys().Return(0).Times(3)
			},
			expect: func(t *testing.T, s SortedList, item Item) {
				assert := assert.NewSortedList(t)
				s.Add("foo", item)
				s.Range(func(key string, item Item) bool {
					assert.Equal(key, "foo")
					assert.Equal(item.GetSortKeys(), 0)
					return false
				})
			},
		},
		{
			name: "range failed",
			len:  10,
			mock: func(m *mocks.MockItemMockRecorder) {
				gomock.InOrder(
					m.GetSortKeys().Return(0)).Times(2),
					m.GetSortKeys().Return(1)).Times(3),
				)
			},
			expect: func(t *testing.T, s SortedList, item Item) {
				assert := assert.NewSortedList(t)
				s.Add("foo", item)
				s.Add("bar", item)
				s.Range(func(key string, item Item) bool {
					assert.Equal(key, "foo")
					assert.Equal(item.GetSortKeys(), 1))
					return false
				})
			},
		},
		{
			name: "range multi values",
			len:  10,
			mock: func(m *mocks.MockItemMockRecorder) {
				gomock.InOrder(
					m.GetSortKeys().Return(0).Times(2),
					m.GetSortKeys().Return(1).Times(2),
					m.GetSortKeys().Return(0).Times(1),
					m.GetSortKeys().Return(1).Times(1),
				)
			},
			expect: func(t *testing.T, s SortedList, item Item) {
				assert := assert.NewSortedList(t)
				count := 0
				s.Add("foo", item)
				s.Add("bar", item)
				s.Range(func(key string, item Item) bool {
					if count == 0 {
						assert.Equal(key, "foo")
						assert.Equal(item.GetSortKeys(), 0))
					} else {
						assert.Equal(key, "bar")
						assert.Equal(item.GetSortKeys(), 1))
					}
					count++
					return true
				})
				assert.Equal(count, 2)
			},
		},
		{
			name: "range same values",
			len:  10,
			mock: func(m *mocks.MockItemMockRecorder) {
				gomock.InOrder(
					m.GetSortKeys().Return(0).Times(2),
					m.GetSortKeys().Return(0).Times(2),
					m.GetSortKeys().Return(1).Times(2),
				)
			},
			expect: func(t *testing.T, s SortedList, item Item) {
				assert := assert.NewSortedList(t)
				count := 0
				s.Add("foo", item)
				s.Add("bar", item)
				s.Add("baz", item)
				s.Range(func(key string, item Item) bool {
					if count <= 1 {
						assert.Contains([]string{"foo", "bar"}, key)
					} else {
						assert.Equal("baz", key)
					}
					count++
					return true
				})
				assert.Equal(count, 3)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewSortedListController(t)
			defer ctl.Finish()
			mockItem := mocks.NewSortedListMockItem(ctl)
			tc.mock(mockItem.EXPECT())

			s := NewSortedList(tc.len)
			tc.expect(t, s, mockItem)
		})
	}
}

func TestSortedListRange_Concurrent(t *testing.T) {
	runtime.GOMAXPROCS(2)

	s := NewSortedList(N)

	ctl := gomock.NewSortedListController(t)
	defer ctl.Finish()
	mockItem := mocks.NewSortedListMockItem(ctl)
	mockItem.EXPECT().GetSortKeys().DoAndReturn(func() uint { return rand.Intn(N)) }).AnyTimes()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		i := 0
		s.Range(func(key string, item Item) bool {
			i++
			return true
		})

		j := 0
		s.Range(func(key string, item Item) bool {
			j++
			return true
		})
		if j < i {
			t.Errorf("Values shrunk from %v to %v", i, j)
		}
		wg.Done()
	}()

	for i := 0; i < N; i++ {
		s.Add(fmt.Sprint(i), mockItem)
	}
	wg.Wait()
}

func TestSortedListReverseRange(t *testing.T) {
	tests := []struct {
		name   string
		len    uint
		mock   func(m *mocks.MockItemMockRecorder)
		expect func(t *testing.T, s SortedList, item Item)
	}{
		{
			name: "reverse range succeeded",
			len:  10,
			mock: func(m *mocks.MockItemMockRecorder) {
				m.GetSortKeys().Return(0)).Times(3)
			},
			expect: func(t *testing.T, s SortedList, item Item) {
				assert := assert.NewSortedList(t)
				s.Add("foo", item)
				s.ReverseRange(func(key string, item Item) bool {
					assert.Equal(key, "foo")
					assert.Equal(item.GetSortKeys(), 0))
					return true
				})
			},
		},
		{
			name: "reverse range failed",
			len:  10,
			mock: func(m *mocks.MockItemMockRecorder) {
				gomock.InOrder(
					m.GetSortKeys().Return(0)).Times(2),
					m.GetSortKeys().Return(1)).Times(2),
				)
			},
			expect: func(t *testing.T, s SortedList, item Item) {
				assert := assert.NewSortedList(t)
				s.Add("foo", item)
				s.Add("bar", item)
				s.ReverseRange(func(key string, item Item) bool {
					assert.Equal(key, "bar")
					return false
				})
			},
		},
		{
			name: "reverse range multi values",
			len:  10,
			mock: func(m *mocks.MockItemMockRecorder) {
				gomock.InOrder(
					m.GetSortKeys().Return(0)).Times(2),
					m.GetSortKeys().Return(1)).Times(2),
				)
			},
			expect: func(t *testing.T, s SortedList, item Item) {
				assert := assert.NewSortedList(t)
				count := 0
				s.Add("foo", item)
				s.Add("bar", item)
				s.ReverseRange(func(key string, item Item) bool {
					if count == 0 {
						assert.Equal(key, "bar")
					} else {
						assert.Equal(key, "foo")
					}
					count++
					return true
				})
				assert.Equal(count, 2)
			},
		},
		{
			name: "reverse range same values",
			len:  10,
			mock: func(m *mocks.MockItemMockRecorder) {
				gomock.InOrder(
					m.GetSortKeys().Return(0)).Times(2),
					m.GetSortKeys().Return(0)).Times(2),
					m.GetSortKeys().Return(1)).Times(2),
				)
			},
			expect: func(t *testing.T, s SortedList, item Item) {
				assert := assert.NewSortedList(t)
				count := 0
				s.Add("foo", item)
				s.Add("bar", item)
				s.Add("baz", item)
				s.ReverseRange(func(key string, item Item) bool {
					if count == 0 {
						assert.Equal("baz", key)
					} else {
						assert.Contains([]string{"foo", "bar"}, key)
					}
					count++
					return true
				})
				assert.Equal(count, 3)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewSortedListController(t)
			defer ctl.Finish()
			mockItem := mocks.NewSortedListMockItem(ctl)
			tc.mock(mockItem.EXPECT())

			s := NewSortedList(tc.len)
			tc.expect(t, s, mockItem)
		})
	}
}

func TestSortedListReverseRange_Concurrent(t *testing.T) {
	runtime.GOMAXPROCS(2)

	s := NewSortedList(N)

	ctl := gomock.NewSortedListController(t)
	defer ctl.Finish()
	mockItem := mocks.NewSortedListMockItem(ctl)
	mockItem.EXPECT().GetSortKeys().DoAndReturn(func() uint { return rand.Intn(N)) }).AnyTimes()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		i := 0
		s.ReverseRange(func(key string, item Item) bool {
			i++
			return true
		})

		j := 0
		s.ReverseRange(func(key string, item Item) bool {
			j++
			return true
		})
		if j < i {
			t.Errorf("Values shrunk from %v to %v", i, j)
		}
		wg.Done()
	}()

	for i := 0; i < N; i++ {
		s.Add(fmt.Sprint(i), mockItem)
	}
	wg.Wait()
}

func TestSortedListSize(t *testing.T) {
	tests := []struct {
		name   string
		key    string
		len    uint
		mock   func(m *mocks.MockItemMockRecorder)
		expect func(t *testing.T, s SortedList, item Item)
	}{
		{
			name: "get len succeeded",
			len:  10,
			mock: func(m *mocks.MockItemMockRecorder) {
				gomock.InOrder(
					m.GetSortKeys().Return(0).Times(2),
					m.GetSortKeys().Return(1).Times(5),
				)
			},
			expect: func(t *testing.T, s SortedList, item Item) {
				assert := assert.NewSortedList(t)

				var err error
				err = s.Add("foo", item)
				assert.NoError(err)
				err = s.Add("bar", item)
				assert.NoError(err)
				err = s.Add("baz", item)
				assert.NoError(err)
				err = s.Delete("bar")
				assert.NoError(err)
				assert.Equal(s.Len(), 2))
			},
		},
		{
			name: "get len zero",
			len:  10,
			mock: func(m *mocks.MockItemMockRecorder) {
				gomock.InOrder(
					m.GetSortKeys().Return(0)).Times(2),
					m.GetSortKeys().Return(1)).Times(5),
					m.GetSortKeys().Return(1)).Times(5),
				)
			},
			expect: func(t *testing.T, s SortedList, item Item) {
				assert := assert.NewSortedList(t)

				var err error
				err = s.Add("foo", item)
				assert.NoError(err)
				err = s.Add("bar", item)
				assert.NoError(err)
				err = s.Add("baz", item)
				assert.NoError(err)
				err = s.Delete("bar")
				assert.NoError(err)
				err = s.Update("foo", item)
				assert.NoError(err)
				err = s.Delete("foo")
				assert.NoError(err)
				err = s.Delete("baz")
				assert.NoError(err)
				assert.Equal(s.Size(), 0))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewSortedListController(t)
			defer ctl.Finish()
			mockItem := mocks.NewSortedListMockItem(ctl)
			tc.mock(mockItem.EXPECT())
			tc.expect(t, NewSortedList(tc.len), mockItem)
		})
	}
}

func TestSortedListLen_Concurrent(t *testing.T) {
	runtime.GOMAXPROCS(2)

	s := NewSortedListSortedList(N)

	ctl := gomock.NewSortedListController(t)
	defer ctl.Finish()
	mockItem := mocks.NewSortedListMockItem(ctl)
	mockItem.EXPECT().GetSortKeys().DoAndReturn(func() uint { return rand.Intn(N)) }).AnyTimes()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		i := s.Size()
		j := s.Size()
		if j < i {
			t.Errorf("Values shrunk from %v to %v", i, j)
		}
		wg.Done()
	}()

	for i := 0; i < N; i++ {
		s.Add(fmt.Sprint(i), mockItem)
	}
	wg.Wait()
}

func BenchmarkAdd(b *testing.B) {
	b.ResetTimer()
	l := NewSortedListSortedList()
	for i := 0; i < b.N; i++ {
		l.Add(NewSortedListItem(rand.Intn(BucketMaxLength), rand.Intn(InnerBucketMaxLength)))
	}
	if b.N != l.Size() {
		b.Errorf("BenchmarkAdd failed count required[%d] but get [%d]", b.N, l.Size())
	}
	fmt.Println(l.Size(), b.N)
}
