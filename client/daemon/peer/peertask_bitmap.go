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

package peer

import (
	"go.uber.org/atomic"
)

type Bitmap struct {
	bits    []byte
	cap     int32
	settled atomic.Int32
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
	for i >= b.cap {
		b.bits = append(b.bits, make([]byte, b.cap/8)...)
		b.cap *= 2
	}
	b.settled.Inc()
	b.bits[i/8] |= 1 << uint(7-i%8)
}

func (b *Bitmap) Clean(i int32) {
	b.settled.Dec()
	b.bits[i/8] ^= 1 << uint(7-i%8)
}

func (b *Bitmap) Settled() int32 {
	return b.settled.Load()
}

func (b *Bitmap) Sets(xs ...int32) {
	for _, x := range xs {
		b.Set(x)
	}
}
