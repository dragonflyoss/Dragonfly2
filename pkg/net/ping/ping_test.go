/*
 *     Copyright 2023 The Dragonfly Authors
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

package ping

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPing(t *testing.T) {
	ipv4Stat, err := Ping("127.0.0.1")
	assert.Nil(t, err)
	assert.Equal(t, ipv4Stat.PacketLoss, float64(0))
	assert.Equal(t, len(ipv4Stat.Rtts), 1)

	ipv6Stat, err := Ping("::1")
	assert.Nil(t, err)
	assert.Equal(t, ipv6Stat.PacketLoss, float64(0))
	assert.Equal(t, len(ipv6Stat.Rtts), 1)

	_, err = Ping("foo")
	assert.Error(t, err)
}
