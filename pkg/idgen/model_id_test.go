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

package idgen

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGNNModelIDV1(t *testing.T) {
	tests := []struct {
		name     string
		ip       string
		hostname string
		expect   func(t *testing.T, d string)
	}{
		{
			name:     "generate GNNModelID",
			ip:       "127.0.0.1",
			hostname: "foo",
			expect: func(t *testing.T, d string) {
				assert := assert.New(t)
				assert.Equal(d, "0c1cfa1cf4b2f58b0e632dca66537cae6596453ec793c38bb14b0de4fa232474")
			},
		},
		{
			name:     "generate GNNModelID with empty ip",
			ip:       "",
			hostname: "foo",
			expect: func(t *testing.T, d string) {
				assert := assert.New(t)
				assert.Equal(d, "10ad70f3d95e523e4d9f6d830ea92b96bb9a8c91da76c135bc66208fb744454c")
			},
		},
		{
			name:     "generate GNNModelID with empty host",
			ip:       "127.0.0.1",
			hostname: "",
			expect: func(t *testing.T, d string) {
				assert := assert.New(t)
				assert.Equal(d, "562a69955f8592589d5ed747888c8c3e9d81420657b7bd33847b5bb2d1d3db4c")
			},
		},
		{
			name:     "generate GNNModelID with zero clusterID",
			ip:       "127.0.0.1",
			hostname: "127.0.0.1",
			expect: func(t *testing.T, d string) {
				assert := assert.New(t)
				assert.Equal(d, "b057d986d82d071f356e13e6f3042b14fe182d57b801a211fa9f21c76ba5290b")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, GNNModelIDV1(tc.ip, tc.hostname))
		})
	}
}

func TestMLPModelIDV1(t *testing.T) {
	tests := []struct {
		name     string
		ip       string
		hostname string
		expect   func(t *testing.T, d string)
	}{
		{
			name:     "generate MLPModelID",
			ip:       "127.0.0.1",
			hostname: "foo",
			expect: func(t *testing.T, d string) {
				assert := assert.New(t)
				assert.Equal(d, "2ba6ab2e9d9eec939b98890c095891aef9864d88558b7b3727fb05ae87d6e037")
			},
		},
		{
			name:     "generate MLPModelID with empty ip",
			ip:       "",
			hostname: "foo",
			expect: func(t *testing.T, d string) {
				assert := assert.New(t)
				assert.Equal(d, "6639d7f1cfa7842016ba5b0a19bf03930ff85d406e6f7763bd4ff88774400298")
			},
		},
		{
			name:     "generate MLPModelID with empty host",
			ip:       "127.0.0.1",
			hostname: "",
			expect: func(t *testing.T, d string) {
				assert := assert.New(t)
				assert.Equal(d, "3b40fd716824d6fc0d5a0f2eff2eb051c526b75a29d4c82a1b2d1174f6db4e7f")
			},
		},
		{
			name:     "generate MLPModelID with zero clusterID",
			ip:       "127.0.0.1",
			hostname: "127.0.0.1",
			expect: func(t *testing.T, d string) {
				assert := assert.New(t)
				assert.Equal(d, "16e2fe757406d847974f711ebe8285df132e5f4f99c297b1bd16b952fe7eee2a")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, MLPModelIDV1(tc.ip, tc.hostname))
		})
	}
}
