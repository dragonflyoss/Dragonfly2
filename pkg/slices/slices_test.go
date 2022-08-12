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

package slices

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestContains(t *testing.T) {
	assert := assert.New(t)
	assert.True(Contains([]int{1, 2, 3}, 1))
	assert.False(Contains([]int{1, 2, 3}, 4))
	assert.True(Contains([]string{"a", "b", "c"}, "a"))
	assert.False(Contains([]string{"a", "b", "c"}, "d"))
}

func TestFindDuplicate(t *testing.T) {
	assert := assert.New(t)
	var (
		dupi  int
		dups  string
		found bool
	)
	dupi, found = FindDuplicate([]int{1, 2, 1, 3})
	assert.True(found)
	assert.Equal(dupi, 1)

	_, found = FindDuplicate([]int{1, 2, 3})
	assert.False(found)

	dups, found = FindDuplicate([]string{"a", "b", "c", "b"})
	assert.True(found)
	assert.Equal(dups, "b")

	_, found = FindDuplicate([]string{"a", "b", "c"})
	assert.False(found)
}

func TestRemoveDuplicates(t *testing.T) {
	assert := assert.New(t)
	var (
		ri []int
		rs []string
	)
	ri = RemoveDuplicates([]int{1, 2, 1, 3})
	assert.EqualValues(ri, []int{1, 2, 3})

	ri = RemoveDuplicates([]int{1, 2, 3})
	assert.EqualValues(ri, []int{1, 2, 3})

	rs = RemoveDuplicates([]string{"a", "b", "c", "b"})
	assert.EqualValues(rs, []string{"a", "b", "c"})

	rs = RemoveDuplicates([]string{"a", "b", "c"})
	assert.EqualValues(rs, []string{"a", "b", "c"})
}
