/*
 *     Copyright 2024 The Dragonfly Authors
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

package redis

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_IsEnabled(t *testing.T) {
	tests := []struct {
		name   string
		addrs  []string
		expect func(t *testing.T, ok bool)
	}{
		{
			name:  "check redis is enabled",
			addrs: []string{"172.0.0.1"},
			expect: func(t *testing.T, ok bool) {
				assert := assert.New(t)
				assert.True(ok)
			},
		},
		{
			name:  "addrs is empty",
			addrs: []string{},
			expect: func(t *testing.T, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, IsEnabled(tc.addrs))
		})
	}
}

func Test_MakeNamespaceKeyInManager(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
		expect    func(t *testing.T, s string)
	}{
		{
			name:      "make namespace key in manager",
			namespace: "namespace",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:namespace")
			},
		},
		{
			name:      "namespace is empty",
			namespace: "",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:")
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, MakeNamespaceKeyInManager(tc.namespace))
		})
	}
}

func Test_MakeKeyInManager(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
		id        string
		expect    func(t *testing.T, s string)
	}{
		{
			name:      "make key in manager",
			namespace: "namespace",
			id:        "foo",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:namespace:foo")
			},
		},
		{
			name:      "namespace is empty",
			namespace: "",
			id:        "foo",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager::foo")
			},
		},
		{
			name:      "key is empty",
			namespace: "namespace",
			id:        "",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:namespace:")
			},
		},
		{
			name:      "namespace and key are empty",
			namespace: "",
			id:        "",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager::")
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, MakeKeyInManager(tc.namespace, tc.id))
		})
	}
}

func Test_MakeSeedPeerKeyInManager(t *testing.T) {
	tests := []struct {
		name      string
		clusterID uint
		hostname  string
		ip        string
		expect    func(t *testing.T, s string)
	}{
		{
			name:      "make seed peer key in manager",
			clusterID: 1,
			hostname:  "foo",
			ip:        "127.0.0.1",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:seed-peers:1-foo-127.0.0.1")
			},
		},
		{
			name:      "hostname is empty",
			clusterID: 1,
			hostname:  "",
			ip:        "127.0.0.1",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:seed-peers:1--127.0.0.1")
			},
		},
		{
			name:      "ip is empty",
			clusterID: 1,
			hostname:  "bar",
			ip:        "",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:seed-peers:1-bar-")
			},
		},
		{
			name:      "hostname and ip are empty",
			clusterID: 1,
			hostname:  "",
			ip:        "",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:seed-peers:1--")
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, MakeSeedPeerKeyInManager(tc.clusterID, tc.hostname, tc.ip))
		})
	}
}

func Test_MakeSchedulerKeyInManager(t *testing.T) {
	tests := []struct {
		name      string
		clusterID uint
		hostname  string
		ip        string
		expect    func(t *testing.T, s string)
	}{
		{
			name:      "make scheduler key in manager",
			clusterID: 1,
			hostname:  "bar",
			ip:        "127.0.0.1",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:schedulers:1-bar-127.0.0.1")
			},
		},
		{
			name:      "hostname is empty",
			clusterID: 1,
			hostname:  "",
			ip:        "127.0.0.1",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:schedulers:1--127.0.0.1")
			},
		},
		{
			name:      "ip is empty",
			clusterID: 1,
			hostname:  "bar",
			ip:        "",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:schedulers:1-bar-")
			},
		},
		{
			name:      "hostname and ip are empty",
			clusterID: 1,
			hostname:  "",
			ip:        "",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:schedulers:1--")
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, MakeSchedulerKeyInManager(tc.clusterID, tc.hostname, tc.ip))
		})
	}
}

func Test_MakePeerKeyInManager(t *testing.T) {
	tests := []struct {
		name     string
		hostname string
		ip       string
		expect   func(t *testing.T, s string)
	}{
		{
			name:     "make peer key in manager",
			hostname: "baz",
			ip:       "127.0.0.1",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:peers:baz-127.0.0.1")
			},
		},
		{
			name:     "hostname is empty",
			hostname: "",
			ip:       "127.0.0.1",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:peers:-127.0.0.1")
			},
		},
		{
			name:     "ip is empty",
			hostname: "baz",
			ip:       "",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:peers:baz-")
			},
		},
		{
			name:     "hostname and ip are empty",
			hostname: "",
			ip:       "",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:peers:-")
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, MakePeerKeyInManager(tc.hostname, tc.ip))
		})
	}
}

func Test_MakeSeedPeersKeyForPeerInManager(t *testing.T) {
	tests := []struct {
		name     string
		hostname string
		ip       string
		expect   func(t *testing.T, s string)
	}{
		{
			name:     "make seed peer key for peer in manager",
			hostname: "bar",
			ip:       "127.0.0.1",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:peers:bar-127.0.0.1:seed-peers")
			},
		},
		{
			name:     "hostname is empty",
			hostname: "",
			ip:       "127.0.0.1",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:peers:-127.0.0.1:seed-peers")
			},
		},
		{
			name:     "ip is empty",
			hostname: "bar",
			ip:       "",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:peers:bar-:seed-peers")
			},
		},
		{
			name:     "hostname and ip are empty",
			hostname: "",
			ip:       "",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:peers:-:seed-peers")
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, MakeSeedPeersKeyForPeerInManager(tc.hostname, tc.ip))
		})
	}
}

func Test_MakeSchedulersKeyForPeerInManager(t *testing.T) {
	tests := []struct {
		name     string
		hostname string
		ip       string
		version  string
		expect   func(t *testing.T, s string)
	}{
		{
			name:     "make scheduler key for peer in manager",
			hostname: "bar",
			ip:       "127.0.0.1",
			version:  "0.1.0",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:peers:bar-127.0.0.1-0.1.0:schedulers")
			},
		},
		{
			name:     "hostname is empty",
			hostname: "",
			ip:       "127.0.0.1",
			version:  "0.1.0",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:peers:-127.0.0.1-0.1.0:schedulers")
			},
		},
		{
			name:     "ip is empty",
			hostname: "bar",
			ip:       "",
			version:  "0.1.0",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:peers:bar--0.1.0:schedulers")
			},
		},
		{
			name:     "version is empty",
			hostname: "bar",
			ip:       "127.0.0.1",
			version:  "",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:peers:bar-127.0.0.1-:schedulers")
			},
		},
		{
			name:     "hostname, ip and version are empty",
			hostname: "",
			ip:       "",
			version:  "",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:peers:--:schedulers")
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, MakeSchedulersKeyForPeerInManager(tc.hostname, tc.ip, tc.version))
		})
	}
}

func Test_MakeApplicationsKeyInManager(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, s string)
	}{
		{
			name: "make applications key in manager",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "manager:applications")
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, MakeApplicationsKeyInManager())
		})
	}
}

func Test_MakeNamespaceKeyInScheduler(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
		expect    func(t *testing.T, s string)
	}{
		{
			name:      "make namespace key in scheduler",
			namespace: "baz",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "scheduler:baz")
			},
		},
		{
			name:      "namespace is empty",
			namespace: "",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "scheduler:")
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, MakeNamespaceKeyInScheduler(tc.namespace))
		})
	}
}

func Test_MakeKeyInScheduler(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
		id        string
		expect    func(t *testing.T, s string)
	}{
		{
			name:      "make key in scheduler",
			namespace: "bas",
			id:        "id",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "scheduler:bas:id")
			},
		},
		{
			name:      "namespace is empty",
			namespace: "",
			id:        "id",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "scheduler::id")
			},
		},
		{
			name:      "id is empty",
			namespace: "bas",
			id:        "",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "scheduler:bas:")
			},
		},
		{
			name:      "namespace and id are empty",
			namespace: "",
			id:        "",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "scheduler::")
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, MakeKeyInScheduler(tc.namespace, tc.id))
		})
	}
}
