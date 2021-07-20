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

package manager

import (
	"testing"

	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	"d7y.io/dragonfly/v2/pkg/rpc/manager"
	"d7y.io/dragonfly/v2/scheduler/config"
	testifyassert "github.com/stretchr/testify/assert"
)

func TestCDNHostsToServers(t *testing.T) {
	mockServerInfo := &manager.CDN{
		HostName:     "foo",
		Port:         8002,
		DownloadPort: 8001,
	}

	tests := []struct {
		name   string
		hosts  []*config.CDN
		expect func(t *testing.T, data interface{})
	}{
		{
			name: "normal conversion",
			hosts: []*config.CDN{
				{
					HostName:     "foo",
					Port:         8002,
					DownloadPort: 8001,
				},
			},
			expect: func(t *testing.T, data interface{}) {
				assert := testifyassert.New(t)
				assert.EqualValues(map[string]*manager.CDN{
					"foo": mockServerInfo,
				}, data)
			},
		},
		{
			name:  "hosts is empty",
			hosts: []*config.CDN{},
			expect: func(t *testing.T, data interface{}) {
				assert := testifyassert.New(t)
				assert.Empty(data)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			data := cdnHostsToServers(tc.hosts)
			tc.expect(t, data)
		})
	}
}

func TestCDNHostsToNetAddrs(t *testing.T) {
	tests := []struct {
		name   string
		hosts  []*config.CDN
		expect func(t *testing.T, data interface{})
	}{
		{
			name: "normal conversion",
			hosts: []*config.CDN{
				{
					HostName:     "foo",
					IP:           "127.0.0.1",
					Port:         8002,
					DownloadPort: 8001,
				},
			},
			expect: func(t *testing.T, data interface{}) {
				assert := testifyassert.New(t)
				assert.EqualValues([]dfnet.NetAddr{
					{
						Type: "tcp",
						Addr: "127.0.0.1:8002",
					},
				}, data)
			},
		},
		{
			name: "host ip is empty",
			hosts: []*config.CDN{
				{
					HostName:     "foo",
					Port:         8002,
					DownloadPort: 8001,
				},
			},
			expect: func(t *testing.T, data interface{}) {
				assert := testifyassert.New(t)
				assert.EqualValues([]dfnet.NetAddr{
					{
						Type: "tcp",
						Addr: ":8002",
					},
				}, data)
			},
		},
		{
			name:  "hosts is empty",
			hosts: []*config.CDN{},
			expect: func(t *testing.T, data interface{}) {
				assert := testifyassert.New(t)
				assert.Empty(data)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			data := cdnHostsToNetAddrs(tc.hosts)
			tc.expect(t, data)
		})
	}
}
