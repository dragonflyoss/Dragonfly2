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
		port     int32
		expect   func(t *testing.T, d string)
	}{
		{
			name:     "generate HostID",
			ip:       "127.0.0.1",
			hostname: "foo",
			port:     8000,
			expect: func(t *testing.T, d string) {
				assert := assert.New(t)
				assert.Equal(d, "b4b84f2b6234738dae4fc7aec86ddd7f7e329bfb944b2a3b9914427843955d92")
			},
		},
		{
			name:     "generate HostID with empty ip",
			ip:       "",
			hostname: "foo",
			port:     8000,
			expect: func(t *testing.T, d string) {
				assert := assert.New(t)
				assert.Equal(d, "96d9be07dba6c87ffe290176c694182b7438f47c22cbef49f199c91a713cccc9")
			},
		},
		{
			name:     "generate HostID with empty host",
			ip:       "127.0.0.1",
			hostname: "",
			port:     8000,
			expect: func(t *testing.T, d string) {
				assert := assert.New(t)
				assert.Equal(d, "82d3b0c2c9d99d3c3b4955a37847e263bea9804713457a0524c37ff460aa2387")
			},
		},
		{
			name:     "generate HostID with zero port",
			ip:       "127.0.0.1",
			hostname: "foo",
			port:     0,
			expect: func(t *testing.T, d string) {
				assert := assert.New(t)
				assert.Equal(d, "eb1959ff4577621851cfb18bd55bc0870c5dfea03c0a45c54de7c44834b344de")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, HostIDV2(tc.ip, tc.hostname, tc.port))
		})
	}
}
