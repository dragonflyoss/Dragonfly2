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

package jsonutils

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMarshal(t *testing.T) {
	v, err := Marshal("hello")
	assert.NoError(t, err)
	assert.Equal(t, "\"hello\"", v)

	v, err = Marshal(1)
	fmt.Printf("int value:%s\n", v)
	assert.NoError(t, err)
	assert.Equal(t, "1", v)

	v, err = Marshal(struct {
		A int
		B string
	}{100, "welcome您!"})

	assert.NoError(t, err)
	fmt.Printf("struct value:%s\n", v)
	assert.NotEmpty(t, v)

}
