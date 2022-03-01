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

package rpc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRateLimiter(t *testing.T) {
	limiter := NewLimiter(&TokenLimit{
		Limit: 5,
		Burst: 2})
	assert.False(t, limiter.Limit())
	assert.False(t, limiter.Limit())
	// rate limit
	assert.True(t, limiter.Limit())
	time.Sleep(404 * time.Millisecond)
	// generate 2 ticket
	assert.False(t, limiter.Limit())
	assert.False(t, limiter.Limit())
	assert.True(t, limiter.Limit())
}
