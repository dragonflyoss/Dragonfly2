/*
 *     Copyright 2022 The Dragonfly Authors
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

package math

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMax(t *testing.T) {
	assert := assert.New(t)
	assert.Equal(1, Max(1))
	assert.Equal(3, Max(1, 2, 3))
	assert.Equal(int8(3), Max(int8(1), int8(2), int8(3)))
	assert.Equal(int16(3), Max(int16(1), int16(2), int16(3)))
	assert.Equal(int32(3), Max(int32(1), int32(2), int32(3)))
	assert.Equal(int64(3), Max(int64(1), int64(2), int64(3)))
	assert.Equal(uint(3), Max(uint(1), uint(2), uint(3)))
	assert.Equal(uint8(3), Max(uint8(1), uint8(2), uint8(3)))
	assert.Equal(uint16(3), Max(uint16(1), uint16(2), uint16(3)))
	assert.Equal(uint32(3), Max(uint32(1), uint32(2), uint32(3)))
	assert.Equal(uint64(3), Max(uint64(1), uint64(2), uint64(3)))
	assert.Equal(float32(1.3), Max(float32(1.1), float32(1.2), float32(1.3)))
	assert.Equal(float64(1.3), Max(1.1, 1.2, 1.3))
	assert.Equal("c", Max("a", "b", "c"))
}

func TestMin(t *testing.T) {
	assert := assert.New(t)
	assert.Equal(1, Min(1))
	assert.Equal(1, Min(1, 2, 3))
	assert.Equal(1, Min(3, 2, 1))
	assert.Equal(int8(1), Min(int8(1), int8(2), int8(3)))
	assert.Equal(int16(1), Min(int16(1), int16(2), int16(3)))
	assert.Equal(int32(1), Min(int32(1), int32(2), int32(3)))
	assert.Equal(int64(1), Min(int64(1), int64(2), int64(3)))
	assert.Equal(uint(1), Min(uint(1), uint(2), uint(3)))
	assert.Equal(uint8(1), Min(uint8(1), uint8(2), uint8(3)))
	assert.Equal(uint16(1), Min(uint16(1), uint16(2), uint16(3)))
	assert.Equal(uint32(1), Min(uint32(1), uint32(2), uint32(3)))
	assert.Equal(uint64(1), Min(uint64(1), uint64(2), uint64(3)))
	assert.Equal(float32(1.1), Min(float32(1.1), float32(1.2), float32(1.3)))
	assert.Equal(float64(1.1), Min(1.1, 1.2, 1.3))
	assert.Equal("a", Min("a", "b", "c"))
}
