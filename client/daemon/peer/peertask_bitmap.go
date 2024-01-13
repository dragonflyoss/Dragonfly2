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
	"sync"

	"github.com/bits-and-blooms/bitset"
)

type Bitmap struct {
	bits *bitset.BitSet
	mu   sync.RWMutex
}

func NewBitmap() *Bitmap {
	return &Bitmap{
		bits: bitset.New(8),
	}
}

func NewBitmapWithCap(c int32) *Bitmap {
	return &Bitmap{
		bits: bitset.New(uint(c)),
	}
}

func (b *Bitmap) IsSet(i int32) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.bits.Test(uint(i))
}

func (b *Bitmap) Set(i int32) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.bits.Set(uint(i))
}

func (b *Bitmap) Clean(i int32) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.bits.Clear(uint(i))
}

func (b *Bitmap) Settled() int32 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return int32(b.bits.Count())
}

func (b *Bitmap) Sets(xs ...int32) {
	for _, x := range xs {
		b.Set(x)
	}
}
