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

package supervisor_test

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/scheduler/supervisor"
)

func TestHost_New(t *testing.T) {
	tests := []struct {
		name   string
		host   *supervisor.Host
		expect func(t *testing.T, host *supervisor.Host)
	}{
		{
			name: "create by normal config",
			host: supervisor.NewClientHost("main", "127.0.0.1", "Client", 8080, 8081, "", "", ""),
			expect: func(t *testing.T, host *supervisor.Host) {
				assert := assert.New(t)
				assert.Equal("main", host.UUID)
			},
		},
		{
			name: "create CDN by normal config",
			host: supervisor.NewCDNHost("main", "127.0.0.1", "Client", 8080, 8081, "", "", ""),
			expect: func(t *testing.T, host *supervisor.Host) {
				assert := assert.New(t)
				assert.Equal("main", host.UUID)
			},
		},
		{
			name: "create by special symbols",
			host: supervisor.NewClientHost("⁂⁎♜♝♞⁑（๑ `▽´๑)", "127.0.0.1", "Client", 8080, 8081, "", "", ""),
			expect: func(t *testing.T, host *supervisor.Host) {
				assert := assert.New(t)
				assert.Equal("⁂⁎♜♝♞⁑（๑ `▽´๑)", host.UUID)
			},
		},
		{
			name: "create by error address",
			host: supervisor.NewClientHost("host", "0.0.0.0", "Client", 8080, 8080, "", "", ""),
			expect: func(t *testing.T, host *supervisor.Host) {
				assert := assert.New(t)
				assert.Equal("host", host.UUID)
			},
		},
		{
			name: "create with geography information",
			host: supervisor.NewClientHost("host", "127.0.0.1", "Client", 8080, 8081, "goagle", "microsaft", "facebaok"),
			expect: func(t *testing.T, host *supervisor.Host) {
				assert := assert.New(t)
				assert.Equal("host", host.UUID)
				assert.Equal("goagle", host.SecurityDomain)
				assert.Equal("microsaft", host.Location)
				assert.Equal("facebaok", host.IDC)
			},
		},
		{
			name: "create by error address",
			host: supervisor.NewClientHost("host", "-1.257.w.-0", "Client", -100, 29000, "", "", ""),
			expect: func(t *testing.T, host *supervisor.Host) {
				assert := assert.New(t)
				assert.Equal("host", host.UUID)
			},
		},
		{
			name: "create by normal config",
			host: supervisor.NewClientHost("host", "127.0.0.1", "Client", 8080, 8081, "", "", ""),
			expect: func(t *testing.T, host *supervisor.Host) {
				assert := assert.New(t)
				assert.Equal("host", host.UUID)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, tc.host)
		})
	}
}

func TestHostManager_New(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, hostManager supervisor.HostManager)
	}{
		{
			name: "simple create",
			expect: func(t *testing.T, hostManager supervisor.HostManager) {
				assert := assert.New(t)
				assert.NotNil(hostManager)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			hostManager := supervisor.NewHostManager()
			tc.expect(t, hostManager)
		})
	}
}

func TestHostManager_Get(t *testing.T) {
	tests := []struct {
		name   string
		number int
		fetch  int
		expect func(t *testing.T, host *supervisor.Host, success bool)
	}{
		{
			name:   "fetch first host",
			number: 3,
			fetch:  0,
			expect: func(t *testing.T, host *supervisor.Host, success bool) {
				assert := assert.New(t)
				assert.Equal("0", host.UUID)
				assert.True(success)
			},
		},
		{
			name:   "fetch last host",
			number: 3,
			fetch:  2,
			expect: func(t *testing.T, host *supervisor.Host, success bool) {
				assert := assert.New(t)
				assert.Equal("2", host.UUID)
				assert.True(success)
			},
		},
		{
			name:   "fetch not exist host",
			number: 3,
			fetch:  -1,
			expect: func(t *testing.T, host *supervisor.Host, success bool) {
				assert := assert.New(t)
				assert.Equal((*supervisor.Host)(nil), host)
				assert.False(success)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			hostManager := supervisor.NewHostManager()
			for i := 0; i < tc.number; i++ {
				index := strconv.Itoa(i)
				host := mockAHost(index)
				hostManager.Add(host)
			}
			host, success := hostManager.Get(strconv.Itoa(tc.fetch))
			tc.expect(t, host, success)
		})
	}
}

func TestHostManager_Delete(t *testing.T) {
	tests := []struct {
		name   string
		number int
		delete int
		expect func(t *testing.T, host *supervisor.Host, success bool)
	}{
		{
			name:   "delete exist host",
			number: 1,
			delete: 0,
			expect: func(t *testing.T, host *supervisor.Host, success bool) {
				assert := assert.New(t)
				assert.Nil(host)
				assert.False(success)
			},
		},
		{
			name:   "delete not exist host",
			number: 1,
			delete: 100,
			expect: func(t *testing.T, host *supervisor.Host, success bool) {
				assert := assert.New(t)
				assert.Nil(host)
				assert.False(success)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			hostManager := supervisor.NewHostManager()
			for i := 0; i < tc.number; i++ {
				index := strconv.Itoa(i)
				host := mockAHost(index)
				hostManager.Add(host)
			}
			hostManager.Delete(strconv.Itoa(tc.delete))
			host, success := hostManager.Get(strconv.Itoa(tc.delete))

			tc.expect(t, host, success)
		})
	}
}

func mockAHost(UUID string) *supervisor.Host {
	host := supervisor.NewClientHost(UUID, "127.0.0.1", "Client", 8080, 8081, "", "", "")
	return host
}
