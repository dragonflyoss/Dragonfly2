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

package iputils

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExternalIPv4(t *testing.T) {
	ip, err := externalIPv4()
	assert.Nil(t, err)
	assert.NotEmpty(t, ip)
}

func Test_sortIP(t *testing.T) {
	var addrs = []net.IP{[]byte("2.3.4.5"), []byte("1.2.3.4"), []byte("3.4.5.6")}
	sortIP(addrs)
	assert.EqualValues(t, addrs, []net.IP{[]byte("1.2.3.4"), []byte("2.3.4.5"), []byte("3.4.5.6")})

	var normalAddrs = []net.IP{[]byte("2.3.4.5"), []byte("1.2.3.4"), []byte("3.4.5.6")}
	var preferAddrs = []net.IP{[]byte("5.3.4.5"), []byte("4.2.3.4"), []byte("6.4.5.6")}
	sortIP(preferAddrs)
	sortIP(normalAddrs)
	assert.EqualValues(t, append(preferAddrs, normalAddrs...), []net.IP{[]byte("4.2.3.4"), []byte("5.3.4.5"), []byte("6.4.5.6"), []byte("1.2.3.4"),
		[]byte("2.3.4.5"), []byte("3.4.5.6")})
}
