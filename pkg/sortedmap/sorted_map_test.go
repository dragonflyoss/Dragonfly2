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
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/pkg/sortedmap/mocks"
)

const N = 10 * 1000

func TestSortedMapAdd(t *testing.T) {
	tests := []struct {
		name   string
		key    string
		len    uint
		mock   func(m *mocks.MockItemMockRecorder)
		expect func(t *testing.T, s SortedMap, key string, item Item)
	}{
		{
			name: "add value succeeded",
			key:  "foo",
			len:  1,
			mock: func(m *mocks.MockItemMockRecorder) {
				m.SortedValue().Return(uint(0)).Times(2)
			},
			expect: func(t *testing.T, s SortedMap, key string, item Item) {
				assert := assert.New(t)
				assert.NoError(s.Add(key, item))
			},
		},
		{
			name: "item sorted value exceeds length",
			key:  "foo",
			len:  1,
			mock: func(m *mocks.MockItemMockRecorder) {
				m.SortedValue().Return(uint(1)).Times(1)
			},
			expect: func(t *testing.T, s SortedMap, key string, item Item) {
				assert := assert.New(t)
				assert.EqualError(s.Add(key, item), "sorted value is illegal")
			},
		},
		{
			name: "add multi values",
			key:  "foo",
			len:  2,
			mock: func(m *mocks.MockItemMockRecorder) {
				m.SortedValue().Return(uint(1)).Times(4)
			},
			expect: func(t *testing.T, s SortedMap, key string, item Item) {
				assert := assert.New(t)
				assert.NoError(s.Add(key, item))
				assert.NoError(s.Add("bar", item))
			},
		},
		{
			name: "add same values",
			key:  "foo",
			len:  1,
			mock: func(m *mocks.MockItemMockRecorder) {
				m.SortedValue().Return(uint(0)).Times(5)
			},
			expect: func(t *testing.T, s SortedMap, key string, item Item) {
				assert := assert.New(t)
				assert.NoError(s.Add(key, item))
				assert.NoError(s.Add(key, item))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			mockItem := mocks.NewMockItem(ctl)
			tc.mock(mockItem.EXPECT())
			tc.expect(t, New(tc.len), tc.key, mockItem)
		})
	}
}

func TestSortedMapAdd_Concurrent(t *testing.T) {
	runtime.GOMAXPROCS(2)

	ctl := gomock.NewController(t)
	defer ctl.Finish()
	mockItem := mocks.NewMockItem(ctl)
	mockItem.EXPECT().SortedValue().DoAndReturn(func() uint { return uint(rand.Intn(N)) }).AnyTimes()

	s := New(N)
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
		t.Errorf("SortedMap is missing element")
	}
}

func TestSortedMapUpdate(t *testing.T) {
	tests := []struct {
		name   string
		key    string
		len    uint
		mock   func(m *mocks.MockItemMockRecorder)
		expect func(t *testing.T, s SortedMap, key string, item Item)
	}{
		{
			name: "update value succeeded",
			key:  "foo",
			len:  1,
			mock: func(m *mocks.MockItemMockRecorder) {
				m.SortedValue().Return(uint(0)).Times(5)
			},
			expect: func(t *testing.T, s SortedMap, key string, item Item) {
				assert := assert.New(t)
				assert.NoError(s.Update(key, item))
			},
		},
		{
			name: "item sorted value exceeds length",
			key:  "foo",
			len:  1,
			mock: func(m *mocks.MockItemMockRecorder) {
				gomock.InOrder(
					m.SortedValue().Return(uint(0)).Times(2),
					m.SortedValue().Return(uint(1)).Times(1),
				)
			},
			expect: func(t *testing.T, s SortedMap, key string, item Item) {
				assert := assert.New(t)
				assert.EqualError(s.Add(key, item), "sorted value is illegal")
			},
		},
		{
			name: "add key dost not exits",
			key:  "foo",
			len:  1,
			mock: func(m *mocks.MockItemMockRecorder) {
				m.SortedValue().Return(uint(0)).Times(3)
			},
			expect: func(t *testing.T, s SortedMap, key string, item Item) {
				assert := assert.New(t)
				assert.EqualError(s.Update("bar", item), "key does not exist")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			mockItem := mocks.NewMockItem(ctl)
			tc.mock(mockItem.EXPECT())
			s := New(tc.len)
			s.Add(tc.key, mockItem)
			tc.expect(t, s, tc.key, mockItem)
		})
	}
}

func TestSortedMapUpdate_Concurrent(t *testing.T) {
	runtime.GOMAXPROCS(2)

	ctl := gomock.NewController(t)
	defer ctl.Finish()
	mockItem := mocks.NewMockItem(ctl)
	mockItem.EXPECT().SortedValue().AnyTimes()

	s := New(N)
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
	s.Range(func(key string, item Item) bool {
		count++
		return true
	})
	if count != len(nums) {
		t.Errorf("SortedMap is missing element")
	}
}

func TestSortedMapDelete(t *testing.T) {
	tests := []struct {
		name   string
		key    string
		len    uint
		mock   func(m *mocks.MockItemMockRecorder)
		expect func(t *testing.T, s SortedMap, key string, item Item)
	}{
		{
			name: "delete value succeeded",
			key:  "foo",
			len:  1,
			mock: func(m *mocks.MockItemMockRecorder) {
				m.SortedValue().Return(uint(0)).Times(3)
			},
			expect: func(t *testing.T, s SortedMap, key string, item Item) {
				assert := assert.New(t)
				assert.NoError(s.Delete(key))
				count := 0
				s.Range(func(key string, item Item) bool {
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
				m.SortedValue().Return(uint(0)).Times(2)
			},
			expect: func(t *testing.T, s SortedMap, key string, item Item) {
				assert := assert.New(t)
				assert.EqualError(s.Delete("bar"), "key does not exist")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			mockItem := mocks.NewMockItem(ctl)
			tc.mock(mockItem.EXPECT())
			s := New(tc.len)
			s.Add(tc.key, mockItem)
			tc.expect(t, s, tc.key, mockItem)
		})
	}
}

func TestSortedMapDelete_Concurrent(t *testing.T) {
	runtime.GOMAXPROCS(2)

	ctl := gomock.NewController(t)
	defer ctl.Finish()
	mockItem := mocks.NewMockItem(ctl)
	mockItem.EXPECT().SortedValue().DoAndReturn(func() uint { return uint(rand.Intn(N)) }).AnyTimes()

	s := New(N)
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
	s.Range(func(key string, item Item) bool {
		count++
		return true
	})
	if count != 0 {
		t.Errorf("SortedMap is redundant elements")
	}
}

func TestSortedMapRange(t *testing.T) {
	tests := []struct {
		name   string
		len    uint
		mock   func(m *mocks.MockItemMockRecorder)
		expect func(t *testing.T, s SortedMap, item Item)
	}{
		{
			name: "range succeeded",
			len:  10,
			mock: func(m *mocks.MockItemMockRecorder) {
				m.SortedValue().Return(uint(0)).Times(3)
			},
			expect: func(t *testing.T, s SortedMap, item Item) {
				assert := assert.New(t)
				s.Add("foo", item)
				s.Range(func(key string, item Item) bool {
					assert.Equal(key, "foo")
					assert.Equal(item.SortedValue(), uint(0))
					return false
				})
			},
		},
		{
			name: "range failed",
			len:  10,
			mock: func(m *mocks.MockItemMockRecorder) {
				gomock.InOrder(
					m.SortedValue().Return(uint(0)).Times(2),
					m.SortedValue().Return(uint(1)).Times(3),
				)
			},
			expect: func(t *testing.T, s SortedMap, item Item) {
				assert := assert.New(t)
				s.Add("foo", item)
				s.Add("bar", item)
				s.Range(func(key string, item Item) bool {
					assert.Equal(key, "foo")
					assert.Equal(item.SortedValue(), uint(1))
					return false
				})
			},
		},
		{
			name: "range multi values",
			len:  10,
			mock: func(m *mocks.MockItemMockRecorder) {
				gomock.InOrder(
					m.SortedValue().Return(uint(0)).Times(2),
					m.SortedValue().Return(uint(1)).Times(2),
					m.SortedValue().Return(uint(0)).Times(1),
					m.SortedValue().Return(uint(1)).Times(1),
				)
			},
			expect: func(t *testing.T, s SortedMap, item Item) {
				assert := assert.New(t)
				count := 0
				s.Add("foo", item)
				s.Add("bar", item)
				s.Range(func(key string, item Item) bool {
					if count == 0 {
						assert.Equal(key, "foo")
						assert.Equal(item.SortedValue(), uint(0))
					} else {
						assert.Equal(key, "bar")
						assert.Equal(item.SortedValue(), uint(1))
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
					m.SortedValue().Return(uint(0)).Times(2),
					m.SortedValue().Return(uint(0)).Times(2),
					m.SortedValue().Return(uint(1)).Times(2),
				)
			},
			expect: func(t *testing.T, s SortedMap, item Item) {
				assert := assert.New(t)
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
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			mockItem := mocks.NewMockItem(ctl)
			tc.mock(mockItem.EXPECT())

			s := New(tc.len)
			tc.expect(t, s, mockItem)
		})
	}
}

func TestSortedMapRange_Concurrent(t *testing.T) {
	runtime.GOMAXPROCS(2)

	s := New(N)

	ctl := gomock.NewController(t)
	defer ctl.Finish()
	mockItem := mocks.NewMockItem(ctl)
	mockItem.EXPECT().SortedValue().DoAndReturn(func() uint { return uint(rand.Intn(N)) }).AnyTimes()

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

func TestSortedMapReverseRange(t *testing.T) {
	tests := []struct {
		name   string
		len    uint
		mock   func(m *mocks.MockItemMockRecorder)
		expect func(t *testing.T, s SortedMap, item Item)
	}{
		{
			name: "reverse range succeeded",
			len:  10,
			mock: func(m *mocks.MockItemMockRecorder) {
				m.SortedValue().Return(uint(0)).Times(3)
			},
			expect: func(t *testing.T, s SortedMap, item Item) {
				assert := assert.New(t)
				s.Add("foo", item)
				s.ReverseRange(func(key string, item Item) bool {
					assert.Equal(key, "foo")
					assert.Equal(item.SortedValue(), uint(0))
					return true
				})
			},
		},
		{
			name: "reverse range failed",
			len:  10,
			mock: func(m *mocks.MockItemMockRecorder) {
				gomock.InOrder(
					m.SortedValue().Return(uint(0)).Times(2),
					m.SortedValue().Return(uint(1)).Times(2),
				)
			},
			expect: func(t *testing.T, s SortedMap, item Item) {
				assert := assert.New(t)
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
					m.SortedValue().Return(uint(0)).Times(2),
					m.SortedValue().Return(uint(1)).Times(2),
				)
			},
			expect: func(t *testing.T, s SortedMap, item Item) {
				assert := assert.New(t)
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
					m.SortedValue().Return(uint(0)).Times(2),
					m.SortedValue().Return(uint(0)).Times(2),
					m.SortedValue().Return(uint(1)).Times(2),
				)
			},
			expect: func(t *testing.T, s SortedMap, item Item) {
				assert := assert.New(t)
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
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			mockItem := mocks.NewMockItem(ctl)
			tc.mock(mockItem.EXPECT())

			s := New(tc.len)
			tc.expect(t, s, mockItem)
		})
	}
}

func TestSortedMapReverseRange_Concurrent(t *testing.T) {
	runtime.GOMAXPROCS(2)

	s := New(N)

	ctl := gomock.NewController(t)
	defer ctl.Finish()
	mockItem := mocks.NewMockItem(ctl)
	mockItem.EXPECT().SortedValue().DoAndReturn(func() uint { return uint(rand.Intn(N)) }).AnyTimes()

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

func TestSortedMapLen(t *testing.T) {
	tests := []struct {
		name   string
		key    string
		len    uint
		mock   func(m *mocks.MockItemMockRecorder)
		expect func(t *testing.T, s SortedMap, item Item)
	}{
		{
			name: "get len succeeded",
			len:  10,
			mock: func(m *mocks.MockItemMockRecorder) {
				gomock.InOrder(
					m.SortedValue().Return(uint(0)).Times(2),
					m.SortedValue().Return(uint(1)).Times(5),
				)
			},
			expect: func(t *testing.T, s SortedMap, item Item) {
				assert := assert.New(t)

				var err error
				err = s.Add("foo", item)
				assert.NoError(err)
				err = s.Add("bar", item)
				assert.NoError(err)
				err = s.Add("baz", item)
				assert.NoError(err)
				err = s.Delete("bar")
				assert.NoError(err)
				assert.Equal(s.Len(), uint(2))
			},
		},
		{
			name: "get len zero",
			len:  10,
			mock: func(m *mocks.MockItemMockRecorder) {
				gomock.InOrder(
					m.SortedValue().Return(uint(0)).Times(2),
					m.SortedValue().Return(uint(1)).Times(5),
					m.SortedValue().Return(uint(1)).Times(5),
				)
			},
			expect: func(t *testing.T, s SortedMap, item Item) {
				assert := assert.New(t)

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
				assert.Equal(s.Len(), uint(0))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			mockItem := mocks.NewMockItem(ctl)
			tc.mock(mockItem.EXPECT())
			tc.expect(t, New(tc.len), mockItem)
		})
	}
}

func TestSortedMapLen_Concurrent(t *testing.T) {
	runtime.GOMAXPROCS(2)

	s := New(N)

	ctl := gomock.NewController(t)
	defer ctl.Finish()
	mockItem := mocks.NewMockItem(ctl)
	mockItem.EXPECT().SortedValue().DoAndReturn(func() uint { return uint(rand.Intn(N)) }).AnyTimes()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		i := s.Len()
		j := s.Len()
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

type item struct{}

func (i *item) SortedValue() uint { return uint(rand.Intn(N)) }

func BenchmarkSortedMapAdd(b *testing.B) {
	s := New(N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Add(fmt.Sprint(i), &item{})
	}
	if uint(b.N) != s.Len() {
		b.Errorf("BenchmarkAdd failed count required[%d] but get [%d]", b.N, s.Len())
	}
}
