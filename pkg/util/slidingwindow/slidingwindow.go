package slidingwindow

import (
	"errors"

	"github.com/willf/bitset"
)

// Window the p2p streaming window.
type Window interface {
	// Size is the size of the window.
	Size() uint

	// Len is the len of the bitset.
	Len() uint

	// Start is the left position of the window.
	Start() uint

	// AddCount increments the accumulated count by n.
	AddCount(n uint)

	// IsFinished returns streaming window execution status.
	IsFinished() bool

	// WindowStatus describe the window status.
	Status() *WindowStatus
}

// WindowStatus describe the window status, which data are available.
type WindowStatus struct {
	size  uint
	start uint
	bitset.BitSet
}

// NewWindow creates a new window.
func NewWindow(len uint, size uint) (Window, error) {
	if len <= size || len == 0 || size == 0 {
		return nil, errors.New("Len and size values are illegal")
	}

	return &WindowStatus{
		size:   size,
		BitSet: *bitset.New(len),
	}, nil
}

func (w *WindowStatus) AddCount(n uint) {
	w.Set(n)

	for w.Test(w.start) {
		w.start += 1
	}
}

func (w *WindowStatus) Size() uint {
	return w.size
}

func (w *WindowStatus) Start() uint {
	return w.start
}

func (w *WindowStatus) Status() *WindowStatus {
	return w
}

func (w *WindowStatus) IsFinished() bool {
	return w.All()
}
