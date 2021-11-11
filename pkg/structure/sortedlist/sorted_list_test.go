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

const N = 1000

type item struct {
	key1 int
	key2 int
}

func NewSortedListItem(key1, key2 int) Item {
	return &item{key1, key2}
}

func (i *item) GetSortKeys() (int, int) {
	return i.key1, i.key2
}

func TestAdd(t *testing.T) {
	l, err := NewSortedList(N)
	assert.Nil(t, err)
	l.Add(NewSortedListItem(1, 2))
	l.Add(NewSortedListItem(2, 2))
	l.Add(NewSortedListItem(2, 3))
	if l.Size() != 3 {
		t.Errorf("TestAdd failed count required[3] but get [%d]", l.Size())
	}
}

func TestDelete(t *testing.T) {
	l, err := NewSortedList(N)
	assert.Nil(t, err)
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
		left:           0,
		right:          0,
		bucketCapacity: N,
		keyMap:         make(map[Item]KeyIndex),
	}
	it := NewSortedListItem(1, 3)
	l.Add(NewSortedListItem(1, 2))
	l.Add(NewSortedListItem(2, 2))
	l.Add(it)
	it.(*item).key1 = 2
	l.Update(it)
	l.Add(NewSortedListItem(2, 3))
	key1, key2, ok := l.getKeyMapKey(it)
	if l.Size() != 4 || key1 != 2 || key2 != 3 || !ok {
		t.Errorf("TestUpdate failed count required[3] but get [%d]", l.Size())
	}
}

func TestRange(t *testing.T) {
	l := &sortedList{
		left:           0,
		right:          0,
		bucketCapacity: N,
		keyMap:         make(map[Item]KeyIndex),
	}
	it := NewSortedListItem(1, 3)
	l.Add(NewSortedListItem(1, 2))
	l.Add(NewSortedListItem(2, 2))
	l.Add(it)
	it.(*item).key1 = 2
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
		left:           0,
		right:          0,
		bucketCapacity: N,
		keyMap:         make(map[Item]KeyIndex),
	}
	it := NewSortedListItem(1, 3)
	l.Add(NewSortedListItem(1, 2))
	l.Add(NewSortedListItem(2, 2))
	l.Add(it)
	it.(*item).key1 = 2
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
		left:           0,
		right:          0,
		bucketCapacity: N,
		keyMap:         make(map[Item]KeyIndex),
	}
	it := NewSortedListItem(1, 3)
	l.Add(NewSortedListItem(1, 2))
	l.Add(NewSortedListItem(2, 2))
	l.Add(it)
	it.(*item).key1 = 2
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
		left:           0,
		right:          0,
		bucketCapacity: N,
		keyMap:         make(map[Item]KeyIndex),
	}
	it := NewSortedListItem(1, 3)
	l.Add(NewSortedListItem(1, 2))
	l.Add(NewSortedListItem(2, 2))
	l.Add(it)
	it.(*item).key1 = 2
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

func TestSortedListAdd_Concurrent(t *testing.T) {
	runtime.GOMAXPROCS(2)

	ctl := gomock.NewController(t)
	defer ctl.Finish()

	s, err := NewSortedList(N)
	assert.Nil(t, err)
	nums := rand.Perm(N)

	var wg sync.WaitGroup
	wg.Add(len(nums))
	for i := 0; i < len(nums); i++ {
		go func(i int) {
			mockItem := mocks.NewMockItem(ctl)
			mockItem.EXPECT().GetSortKeys().DoAndReturn(func() (int, int) { return rand.Intn(N), rand.Intn(N) }).AnyTimes()
			if err := s.Add(mockItem); err != nil {
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

func TestSortedListUpdate_Concurrent(t *testing.T) {
	runtime.GOMAXPROCS(2)

	ctl := gomock.NewController(t)
	defer ctl.Finish()

	s, err := NewSortedList(N)
	assert.Nil(t, err)
	nums := rand.Intn(N)

	var mockItems []*mocks.MockItem
	for i := 0; i < nums; i++ {
		mockItem := mocks.NewMockItem(ctl)
		mockItem.EXPECT().GetSortKeys().AnyTimes()
		s.Add(mockItem)
		mockItems = append(mockItems, mockItem)
	}

	var wg sync.WaitGroup
	wg.Add(nums)
	for i := 0; i < nums; i++ {
		go func(v int) {
			if err := s.Update(mockItems[v]); err != nil {
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
	if count != nums {
		t.Errorf("SortedList is missing element")
	}
}

func TestSortedListDelete_Concurrent(t *testing.T) {
	runtime.GOMAXPROCS(2)

	ctl := gomock.NewController(t)
	defer ctl.Finish()

	s, _ := NewSortedList(N)
	nums := rand.Perm(N)

	var mockItems []*mocks.MockItem
	for i := 0; i < len(nums); i++ {
		mockItem := mocks.NewMockItem(ctl)
		mockItem.EXPECT().GetSortKeys().DoAndReturn(func() (int, int) { return rand.Intn(N), rand.Intn(N) }).AnyTimes()
		s.Add(mockItem)
		mockItems = append(mockItems, mockItem)
	}

	var wg sync.WaitGroup
	wg.Add(len(nums))
	for i := 0; i < len(nums); i++ {
		go func(i int) {
			if err := s.Delete(mockItems[i]); err != nil {
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

func TestSortedListReverseRange_Concurrent(t *testing.T) {
	runtime.GOMAXPROCS(2)

	s, _ := NewSortedList(N)

	ctl := gomock.NewController(t)
	defer ctl.Finish()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		i := 0
		s.ReverseRange(func(item Item) bool {
			i++
			return true
		})

		j := 0
		s.ReverseRange(func(item Item) bool {
			j++
			return true
		})
		if j < i {
			t.Errorf("Values shrunk from %v to %v", i, j)
		}
		wg.Done()
	}()

	for i := 0; i < N; i++ {
		mockItem := mocks.NewMockItem(ctl)
		mockItem.EXPECT().GetSortKeys().DoAndReturn(func() (int, int) { return rand.Intn(N), rand.Intn(N) }).AnyTimes()
		s.Add(mockItem)
	}
	wg.Wait()
}

func TestSortedListLen_Concurrent(t *testing.T) {
	runtime.GOMAXPROCS(2)

	s, _ := NewSortedList(N)

	ctl := gomock.NewController(t)
	defer ctl.Finish()

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
		mockItem := mocks.NewMockItem(ctl)
		mockItem.EXPECT().GetSortKeys().DoAndReturn(func() (int, int) { return rand.Intn(N), rand.Intn(N) }).AnyTimes()
		s.Add(mockItem)
	}
	wg.Wait()
}

func BenchmarkAdd(b *testing.B) {
	b.ResetTimer()
	l, _ := NewSortedList(N)
	for i := 0; i < b.N; i++ {
		l.Add(NewSortedListItem(rand.Intn(N), rand.Intn(N)))
	}
	if b.N != l.Size() {
		b.Errorf("BenchmarkAdd failed count required[%d] but get [%d]", b.N, l.Size())
	}
	fmt.Println(l.Size(), b.N)
}
