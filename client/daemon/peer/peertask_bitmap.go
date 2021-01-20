package peer

type Bitmap struct {
	bits    []byte
	cap     int32
	settled int32
}

func NewBitmap() *Bitmap {
	return &Bitmap{
		bits: make([]byte, 8),
		cap:  8 * 8,
	}
}

func NewBitmapWithCap(c int32) *Bitmap {
	return &Bitmap{
		bits: make([]byte, c),
		cap:  c * 8,
	}
}

func (b *Bitmap) IsSet(i int32) bool {
	if i >= b.cap {
		return false
	}
	return b.bits[i/8]&(1<<uint(7-i%8)) != 0
}

func (b *Bitmap) Set(i int32) {
	if i >= b.cap {
		b.bits = append(b.bits, make([]byte, b.cap/8)...)
		b.cap *= 2
	}
	//if b.IsSet(i) {
	//	return
	//}
	b.settled++
	b.bits[i/8] |= 1 << uint(7-i%8)
}

func (b *Bitmap) Settled() int32 {
	return b.settled
}

//func (b *Bitmap) Clear(i int) {
//	b.bits[i/8] &^= 1 << uint(7-i%8)
//}

func (b *Bitmap) Sets(xs ...int32) {
	for _, x := range xs {
		b.Set(x)
	}
}

//func (b Bitmap) String() string {
//	return hex.EncodeToString(b.bits[:])
//}
