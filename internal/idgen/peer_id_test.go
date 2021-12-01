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

func TestPeerID(t *testing.T) {
	tests := []struct {
		name   string
		ip     string
		expect func(t *testing.T, d interface{})
	}{
		{
			name: "generate PeerID with ipv4",
			ip:   "127.0.0.1",
			expect: func(t *testing.T, d interface{}) {
				assert := assert.New(t)
				assert.Len(d, 52)
			},
		},
		{
			name: "generate PeerID with ipv6",
			ip:   "2001:0db8:3c4d:0015:0000:0000:1a2f:1a2b",
			expect: func(t *testing.T, d interface{}) {
				assert := assert.New(t)
				assert.Len(d, 82)
			},
		},
		{
			name: "generate PeerID with empty string",
			ip:   "",
			expect: func(t *testing.T, d interface{}) {
				assert := assert.New(t)
				assert.Len(d, 43)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			data := PeerID(tc.ip)
			tc.expect(t, data)
		})
	}
}

func TestCDNPeerID(t *testing.T) {
	tests := []struct {
		name   string
		ip     string
		expect func(t *testing.T, d interface{})
	}{
		{
			name: "generate CDNPeerID with ipv4",
			ip:   "127.0.0.1",
			expect: func(t *testing.T, d interface{}) {
				assert := assert.New(t)
				assert.Len(d, 56)
			},
		},
		{
			name: "generate CDNPeerID with ipv6",
			ip:   "2001:0db8:3c4d:0015:0000:0000:1a2f:1a2b",
			expect: func(t *testing.T, d interface{}) {
				assert := assert.New(t)
				assert.Len(d, 86)
			},
		},
		{
			name: "generate CDNPeerID with empty string",
			ip:   "",
			expect: func(t *testing.T, d interface{}) {
				assert := assert.New(t)
				assert.Len(d, 47)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			data := CDNPeerID(tc.ip)
			tc.expect(t, data)
		})
	}
}
