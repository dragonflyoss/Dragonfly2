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

package reachable

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestReachableCheck(t *testing.T) {
	l, err := net.Listen("tcp", ":3000")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	tests := []struct {
		name    string
		address string
		network string
		timeout time.Duration
		expect  func(t *testing.T, err error)
	}{
		{
			name:    "check address",
			address: ":3000",
			network: "tcp",
			timeout: 1 * time.Second,
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
		{
			name:    "check address without network",
			address: ":3000",
			network: "",
			timeout: 1 * time.Second,
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
		{
			name:    "check address without address",
			address: "",
			network: "tcp",
			timeout: 1 * time.Second,
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.Error(err)
			},
		},
		{
			name:    "check invalid address",
			address: "example",
			network: "tcp",
			timeout: 1 * time.Second,
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.Error(err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := New(&Config{
				Address: tc.address,
				Network: tc.network,
				Timeout: tc.timeout,
			})
			tc.expect(t, r.Check())
		})
	}
}
