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

package ifaceutils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsNil(t *testing.T) {
	var eface interface{} = nil
	assert.True(t, IsNil(eface))

	var fun func() = nil
	assert.True(t, IsNil(fun))

	var ch chan int = nil
	assert.True(t, IsNil(ch))

	var pt *int = nil
	assert.True(t, IsNil(pt))

	assert.False(t, IsNil(1))
}

func TestIsZero(t *testing.T) {
	var in int = 0
	assert.True(t, IsZero(in))

	var st = struct {
		x int
		y string
	}{}

	assert.True(t, IsZero(st))

	assert.False(t, IsZero(1))
}
