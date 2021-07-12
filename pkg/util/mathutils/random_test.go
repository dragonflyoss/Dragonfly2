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

package mathutils

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRandBackoff(t *testing.T) {
	initBackOff := 0.1
	maxBackOff := 3.0

	v1 := RandBackoff(initBackOff, maxBackOff, 2, 5)
	fmt.Printf("rand v1: %v\n", v1)

	assert.True(t, v1 <= time.Duration(maxBackOff*float64(time.Second)))
	assert.True(t, v1 >= time.Duration(initBackOff*float64(time.Second)))

	v2 := RandBackoff(initBackOff, maxBackOff, 2, 5)
	fmt.Printf("rand v2: %v\n", v2)

	assert.NotEqual(t, v1, v2)
}
