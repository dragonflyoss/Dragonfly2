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
		name       string
		ip         string
		hostname   string
		isSeedPeer bool
		expect     func(t *testing.T, d string)
	}{
		{
			name:       "generate HostID for peer",
			ip:         "127.0.0.1",
			hostname:   "foo",
			isSeedPeer: false,
			expect: func(t *testing.T, d string) {
				assert := assert.New(t)
				assert.Equal(d, "127.0.0.1-foo")
			},
		},
		{
			name:       "generate HostID for seed peer",
			ip:         "127.0.0.1",
			hostname:   "foo",
			isSeedPeer: true,
			expect: func(t *testing.T, d string) {
				assert := assert.New(t)
				assert.Equal(d, "127.0.0.1-foo-seed")
			},
		},
		{
			name:       "generate HostID with empty ip for seed peer",
			ip:         "",
			hostname:   "foo",
			isSeedPeer: true,
			expect: func(t *testing.T, d string) {
				assert := assert.New(t)
				assert.Equal(d, "-foo-seed")
			},
		},
		{
			name:       "generate HostID with empty host for seed peer",
			ip:         "127.0.0.1",
			hostname:   "",
			isSeedPeer: true,
			expect: func(t *testing.T, d string) {
				assert := assert.New(t)
				assert.Equal(d, "127.0.0.1--seed")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, HostIDV2(tc.ip, tc.hostname, tc.isSeedPeer))
		})
	}
}
