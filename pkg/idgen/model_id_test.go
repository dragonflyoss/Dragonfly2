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
		name      string
		ip        string
		hostname  string
		clusterID uint64
		expect    func(t *testing.T, d string)
	}{
		{
			name:      "generate GNNModelID",
			ip:        "127.0.0.1",
			hostname:  "foo",
			clusterID: 1,
			expect: func(t *testing.T, d string) {
				assert := assert.New(t)
				assert.Equal(d, "1f87cb3e4d63a6dec56a169a61b17c62c342b6d1bfea7bc36110fcee79a881aa")
			},
		},
		{
			name:      "generate GNNModelID with empty ip",
			ip:        "",
			hostname:  "foo",
			clusterID: 1,
			expect: func(t *testing.T, d string) {
				assert := assert.New(t)
				assert.Equal(d, "41a2ad9148f8a0355c0f61573d17312a0fd3fc542ee4d71a82a7e2b29ada645c")
			},
		},
		{
			name:      "generate GNNModelID with empty host",
			ip:        "127.0.0.1",
			hostname:  "",
			clusterID: 1,
			expect: func(t *testing.T, d string) {
				assert := assert.New(t)
				assert.Equal(d, "1ee3838a1a87aae3dd8e718c6dc146b234e3fb1312e75324cb374ea3f340b476")
			},
		},
		{
			name:      "generate GNNModelID with zero clusterID",
			ip:        "127.0.0.1",
			hostname:  "127.0.0.1",
			clusterID: 0,
			expect: func(t *testing.T, d string) {
				assert := assert.New(t)
				assert.Equal(d, "a8db74e81e065ffb255fb9f4c2e26f09851ea795a264098b77ea21715ab3ecd6")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, GNNModelIDV1(tc.ip, tc.hostname, tc.clusterID))
		})
	}
}

func TestMLPModelIDV1(t *testing.T) {
	tests := []struct {
		name      string
		ip        string
		hostname  string
		clusterID uint64
		expect    func(t *testing.T, d string)
	}{
		{
			name:      "generate MLPModelID",
			ip:        "127.0.0.1",
			hostname:  "foo",
			clusterID: 1,
			expect: func(t *testing.T, d string) {
				assert := assert.New(t)
				assert.Equal(d, "b198e604525d8117922f12dde4d7275190948738d60a1d6b03357ae30d2e2ecf")
			},
		},
		{
			name:      "generate MLPModelID with empty ip",
			ip:        "",
			hostname:  "foo",
			clusterID: 1,
			expect: func(t *testing.T, d string) {
				assert := assert.New(t)
				assert.Equal(d, "5b7ba8256ee4fe626cddbadfaa3f655c1581bf05404d60a0c9879e5389bf3c7f")
			},
		},
		{
			name:      "generate MLPModelID with empty host",
			ip:        "127.0.0.1",
			hostname:  "",
			clusterID: 1,
			expect: func(t *testing.T, d string) {
				assert := assert.New(t)
				assert.Equal(d, "a42115f661da4711c7d94a1af9fce24a06a335b6526b4caa1d1d33ffe00625f3")
			},
		},
		{
			name:      "generate MLPModelID with zero clusterID",
			ip:        "127.0.0.1",
			hostname:  "127.0.0.1",
			clusterID: 0,
			expect: func(t *testing.T, d string) {
				assert := assert.New(t)
				assert.Equal(d, "afe4620a10bde10471e8627a5d965d68fc4a15193f8cf23b3be61bce4d91d4c4")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, MLPModelIDV1(tc.ip, tc.hostname, tc.clusterID))
		})
	}
}
