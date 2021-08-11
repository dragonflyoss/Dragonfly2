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

package stringutils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSubString(t *testing.T) {
	var str = "hello,world"

	assert.Equal(t, SubString(str, 0, 5), "hello")

	assert.Equal(t, SubString(str, 1, 5), "ello")

	assert.Equal(t, SubString(str, 1, 1), "")
}

func TestIsBlank(t *testing.T) {
	assert.True(t, IsBlank(""))

	assert.True(t, IsBlank("	  "))

	assert.False(t, IsBlank("x"))
}

func TestIsEmpty(t *testing.T) {
	assert.True(t, IsEmpty(""))

	assert.False(t, IsEmpty(" "))
}

func TestContains(t *testing.T) {
	assert.True(t, ContainsFold([]string{"a", "B"}, "b"))
	assert.False(t, Contains([]string{"a", "B"}, "b"))
}

func TestRandString(t *testing.T) {
	assert.True(t, len(RandString(10)) == 10)
	x := RandString(10)
	y := RandString(10)
	assert.False(t, x == y)
}
