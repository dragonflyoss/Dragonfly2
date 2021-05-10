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

package idgen

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGenerateTaskId(t *testing.T) {
	taskId := TaskID("http://alibaba.com/path/xx", "", nil, "")
	assert.NotEmpty(t, taskId)
}

func TestGenerateTwinsTaskId(t *testing.T) {
	taskId := TwinsTaskID("http://alibaba.com/path/xx", "", nil, "", "peerId")
	assert.NotEmpty(t, taskId)
}
