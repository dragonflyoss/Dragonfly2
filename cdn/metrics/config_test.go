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

package metrics

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig(t *testing.T) {
	tcpConfig := Config{
		Net:  "tcp",
		Addr: "1.1.1.1:22",
	}
	addr, err := net.ResolveTCPAddr(tcpConfig.Net, tcpConfig.Addr)
	assert.Equal(t, tcpConfig.Addr, addr.String())
	assert.Nil(t, err)
	unixConfig := Config{
		Net:  "unix",
		Addr: "/unix/metrics.sock",
	}
	unixAddr, err := net.ResolveUnixAddr(unixConfig.Net, unixConfig.Addr)
	assert.Equal(t, unixConfig.Addr, unixAddr.String())
	assert.Nil(t, err)
}
