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
		w.start++
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
