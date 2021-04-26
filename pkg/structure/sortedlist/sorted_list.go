package sortedlist

import (
	"fmt"
	"sync"
)

const BucketMaxLength = 10000
const InnerBucketMaxLength = 20

type Item interface {
	GetSortKeys() (key1 int, key2 int)
}

type SortedList struct {
	l       sync.RWMutex
	buckets []bucket
	keyMap  map[Item]int
	left    int
	right   int
}

func NewSortedList() *SortedList {
	l := &SortedList{
		left:   0,
		right:  0,
		keyMap: make(map[Item]int),
	}
	return l
}

func (l *SortedList) Add(data Item) (err error) {
	key1, key2 := data.GetSortKeys()
	if key1 > BucketMaxLength || key1 < 0 {
		return fmt.Errorf("sorted list key1 out of range")
	}
	if key2 > InnerBucketMaxLength || key2 < 0 {
		return fmt.Errorf("sorted list key2 out of range")
	}
	l.l.Lock()
	defer l.l.Unlock()
	l.addItem(key1, key2, data)
	return
}
func (l *SortedList) Update(data Item) (err error) {
	key1, key2 := data.GetSortKeys()
	if key1 > BucketMaxLength || key1 < 0 {
		return fmt.Errorf("sorted list key1 out of range")
	}
	if key2 > InnerBucketMaxLength || key2 < 0 {
		return fmt.Errorf("sorted list key2 out of range")
	}

	l.l.Lock()
	defer l.l.Unlock()
	oldKey1, oldKey2, ok := l.getKeyMapKey(data)
	if !ok {
		return
	}

	if key1 == oldKey1 && key2 == oldKey2 {
		return
	}

	l.deleteItem(oldKey1, oldKey2, data)
	l.addItem(key1, key2, data)

	return
}

func (l *SortedList) UpdateOrAdd(data Item) (err error) {
	key1, key2 := data.GetSortKeys()
	if key1 > BucketMaxLength || key1 < 0 {
		return fmt.Errorf("sorted list key1 out of range")
	}
	if key2 > InnerBucketMaxLength || key2 < 0 {
		return fmt.Errorf("sorted list key2 out of range")
	}

	l.l.Lock()
	defer l.l.Unlock()
	oldKey1, oldKey2, ok := l.getKeyMapKey(data)
	if !ok {
		l.addItem(key1, key2, data)
		return
	}

	if key1 == oldKey1 && key2 == oldKey2 {
		return
	}

	l.deleteItem(oldKey1, oldKey2, data)
	l.addItem(key1, key2, data)

	return
}

func (l *SortedList) Delete(data Item) (err error) {
	l.l.Lock()
	defer l.l.Unlock()
	oldKey1, oldKey2, ok := l.getKeyMapKey(data)
	if !ok {
		return
	}
	l.deleteItem(oldKey1, oldKey2, data)
	return
}

func (l *SortedList) Range(fn func(data Item) bool) {
	l.RangeLimit(-1, fn)
}

func (l *SortedList) RangeLimit(limit int ,fn func(Item) bool) {
	if limit == 0 {
		return
	}
	l.l.RLock()
	defer l.l.RUnlock()

	count := 0
	for i := l.left; i<=l.right; i++ {
		buc := l.buckets[i]
		for _, b := range buc.buckets {
			for it := range b {
				if !fn(it) {
					return
				}
				count++
				if limit > 0 && count >= limit {
					return
				}
			}
		}
	}
}

func (l *SortedList) RangeReverse(fn func(data Item) bool) {
	l.RangeReverseLimit(-1, fn)
}

func (l *SortedList) RangeReverseLimit(limit int ,fn func(Item) bool) {
	if limit == 0 {
		return
	}
	l.l.RLock()
	defer l.l.RUnlock()

	count := 0
	for i := l.right; i>=l.left; i-- {
		for j:=len(l.buckets[i].buckets)-1; j>=0; j-- {
			for it := range l.buckets[i].buckets[j] {
				if !fn(it) {
					return
				}
				count++
				if limit > 0 && count >= limit {
					return
				}
			}
		}
	}
}

func (l *SortedList) Size() int {
	l.l.RLock()
	defer l.l.RUnlock()
	return len(l.keyMap)
}

func (l *SortedList) addItem(key1, key2 int, data Item) {
	l.addKey(key1)
	l.buckets[key1].Add(key2, data)
	l.setKeyMapKey(key1, key2, data)
	l.shrink()
}

func (l *SortedList) deleteItem(key1, key2 int, data Item) {
	l.addKey(key1)
	l.buckets[key1].Delete(key2, data)
	l.deleteKeyMapKey(data)
	l.shrink()
}

func (l *SortedList) addKey(key int) {
	for key >= len(l.buckets) {
		l.buckets = append(l.buckets, bucket{})
	}
	if l.right < key {
		l.right = key
	}
	if l.left > key {
		l.left = key
	}
}

func (l *SortedList) shrink() {
	for l.left < l.right && l.buckets[l.left].Size() == 0 {
		l.left++
	}
	for l.left < l.right && l.buckets[l.right].Size() == 0 {
		l.right--
	}
}

func (l *SortedList) setKeyMapKey(key1, key2 int, data Item) {
	l.keyMap[data] = key1*1000 + key2
}

func (l *SortedList) getKeyMapKey(data Item) (key1, key2 int, ok bool) {
	key, ok := l.keyMap[data]
	key1 = key / 1000
	key2 = key % 1000
	return
}

func (l *SortedList) deleteKeyMapKey(data Item) {
	delete(l.keyMap, data)
}

