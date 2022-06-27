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

package strings

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsBlank(t *testing.T) {
	assert.True(t, IsBlank(""))
	assert.True(t, IsBlank("	  "))
	assert.False(t, IsBlank("x"))
}

func TestContains(t *testing.T) {
	assert.True(t, Contains([]string{"a", "B"}, "B"))
	assert.False(t, Contains([]string{"a", "B"}, "b"))
}

func TestUnique(t *testing.T) {
	assert.EqualValues(t, Unique([]string{"a", "B"}), []string{"a", "B"})
	assert.EqualValues(t, Unique([]string{"a", "a", "B", "B"}), []string{"a", "B"})
	assert.EqualValues(t, Unique([]string{"a", "B", "a", "B"}), []string{"a", "B"})
	assert.EqualValues(t, Unique([]string{}), []string{})
	assert.EqualValues(t, Unique([]string{}), []string{})
}
