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

package timeutils

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCurrentTimeMillis(t *testing.T) {
	v1 := CurrentTimeMillis()
	time.Sleep(time.Millisecond * 500)
	v2 := CurrentTimeMillis()
	assert.LessOrEqual(t, v1, v2)
}

func TestSinceInMilliseconds(t *testing.T) {
	tim := time.Now()
	time.Sleep(500 * time.Millisecond)

	assert.GreaterOrEqual(t, SinceInMilliseconds(tim), int64(500))
}
