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

func TestHostIDV1(t *testing.T) {
	tests := []struct {
		name     string
		hostname string
		port     int32
		expect   func(t *testing.T, d string)
	}{
		{
			name:     "generate HostID",
			hostname: "foo",
			port:     8000,
			expect: func(t *testing.T, d string) {
				assert := assert.New(t)
				assert.Equal(d, "foo-8000")
			},
		},
		{
			name:     "generate HostID with empty host",
			hostname: "",
			port:     8000,
			expect: func(t *testing.T, d string) {
				assert := assert.New(t)
				assert.Equal(d, "-8000")
			},
		},
		{
			name:     "generate HostID with zero port",
			hostname: "foo",
			port:     0,
			expect: func(t *testing.T, d string) {
				assert := assert.New(t)
				assert.Equal(d, "foo-0")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, HostIDV1(tc.hostname, tc.port))
		})
	}
}

func TestHostIDV2(t *testing.T) {
	tests := []struct {
		name     string
		ip       string
		hostname string
		expect   func(t *testing.T, d string)
	}{
		{
			name:     "generate HostID",
			ip:       "127.0.0.1",
			hostname: "foo",
			expect: func(t *testing.T, d string) {
				assert := assert.New(t)
				assert.Equal(d, "52727e8408e0ee1f999086f241ec43d5b3dbda666f1a06ef1fcbe75b4e90fa17")
			},
		},
		{
			name:     "generate HostID with empty ip",
			ip:       "",
			hostname: "foo",
			expect: func(t *testing.T, d string) {
				assert := assert.New(t)
				assert.Equal(d, "2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae")
			},
		},
		{
			name:     "generate HostID with empty host",
			ip:       "127.0.0.1",
			hostname: "",
			expect: func(t *testing.T, d string) {
				assert := assert.New(t)
				assert.Equal(d, "12ca17b49af2289436f303e0166030a21e525d266e209267433801a8fd4071a0")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, HostIDV2(tc.ip, tc.hostname))
		})
	}
}
