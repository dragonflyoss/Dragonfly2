package peer

import "testing"

func TestBitmap_Sets(t *testing.T) {
	b := NewBitmap()
	for i := 0; i < 512; i++ {
		b.Set(i)
	}
	b.Sets(2, 3, 3, 4)
	//t.Logf("%s, %d", b.String(), b.IsSettled())
}
