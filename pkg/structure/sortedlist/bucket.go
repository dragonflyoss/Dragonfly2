package sortedlist

type empty struct{}

type bucket struct {
	count int
	buckets []map[Item]empty
}

func (b *bucket) Size() int {
	return b.count
}

func (b *bucket) Add(key int, data Item) {
	for key >= len(b.buckets) {
		b.buckets = append(b.buckets, make(map[Item]empty))
	}
	b.buckets[key][data] = empty{}
	b.count++
	return
}

func (b *bucket) Delete(key int, data Item) {
	for key >= len(b.buckets) {
		b.buckets = append(b.buckets, make(map[Item]empty))
	}
	_, ok := b.buckets[key][data]
	if ok {
		delete(b.buckets[key], data)
		b.count--
	}
	return
}

