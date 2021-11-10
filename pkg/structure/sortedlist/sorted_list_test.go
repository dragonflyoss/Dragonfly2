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
	"testing"
)

type item struct {
	key1 int
	key2 int
}

func newItem(key1, key2 int) *item {
	return &item{key1, key2}
}

func (i *item) GetSortKeys() (int, int) {
	return i.key1, i.key2
}

func TestAdd(t *testing.T) {
	l := NewSortedList()
	l.Add(newItem(1, 2))
	l.Add(newItem(2, 2))
	l.Add(newItem(2, 3))
	if l.Size() != 3 {
		t.Errorf("TestAdd failed count required[3] but get [%d]", l.Size())
	}
}

func TestDelete(t *testing.T) {
	l := NewSortedList()
	it := newItem(1, 3)
	l.Add(newItem(1, 2))
	l.Add(newItem(2, 2))
	l.Add(it)
	l.Add(newItem(2, 3))
	l.Delete(it)
	if l.Size() != 3 {
		t.Errorf("TestDelete failed count required[3] but get [%d]", l.Size())
	}
}

func TestUpdate(t *testing.T) {
	l := NewSortedList()
	it := newItem(1, 3)
	l.Add(newItem(1, 2))
	l.Add(newItem(2, 2))
	l.Add(it)
	it.key1 = 2
	l.Update(it)
	l.Add(newItem(2, 3))
	key1, key2, ok := l.getKeyMapKey(it)
	if l.Size() != 4 || key1 != 2 || key2 != 3 || !ok {
		t.Errorf("TestUpdate failed count required[3] but get [%d]", l.Size())
	}
}

func TestRange(t *testing.T) {
	l := NewSortedList()
	it := newItem(1, 3)
	l.Add(newItem(1, 2))
	l.Add(newItem(2, 2))
	l.Add(it)
	it.key1 = 2
	l.Update(it)
	l.Add(newItem(2, 3))
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
	l := NewSortedList()
	it := newItem(1, 3)
	l.Add(newItem(1, 2))
	l.Add(newItem(2, 2))
	l.Add(it)
	it.key1 = 2
	l.Update(it)
	l.Add(newItem(2, 3))
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
	l := NewSortedList()
	it := newItem(1, 3)
	l.Add(newItem(1, 2))
	l.Add(newItem(2, 2))
	l.Add(it)
	it.key1 = 2
	l.Update(it)
	l.Add(newItem(2, 3))
	key1, key2, ok := l.getKeyMapKey(it)
	if l.Size() != 4 || key1 != 2 || key2 != 3 || !ok {
		t.Errorf("TestRangeReverse failed count required[4] but get [%d]", l.Size())
	}

	count := 0
	l.RangeReverse(func(data Item) bool {
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
	l := NewSortedList()
	it := newItem(1, 3)
	l.Add(newItem(1, 2))
	l.Add(newItem(2, 2))
	l.Add(it)
	it.key1 = 2
	l.Update(it)
	l.Add(newItem(2, 3))
	key1, key2, ok := l.getKeyMapKey(it)
	if l.Size() != 4 || key1 != 2 || key2 != 3 || !ok {
		t.Errorf("TestRangeReverseLimit failed count required[4] but get [%d]", l.Size())
	}

	count := 0
	l.RangeReverseLimit(2, func(data Item) bool {
		it := data.(*item)
		fmt.Println(it.key1, it.key2)
		count++
		return true
	})
	if 2 != count {
		t.Errorf("TestRangeReverseLimit failed count required[4] but get [%d]", l.Size())
	}
}

func BenchmarkAdd(b *testing.B) {
	b.ResetTimer()
	l := NewSortedList()
	for i := 0; i < b.N; i++ {
		l.Add(newItem(rand.Intn(BucketMaxLength), rand.Intn(InnerBucketMaxLength)))
	}
	if b.N != l.Size() {
		b.Errorf("BenchmarkAdd failed count required[%d] but get [%d]", b.N, l.Size())
	}
}
