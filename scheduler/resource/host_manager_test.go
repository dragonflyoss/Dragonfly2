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

package resource

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHostManager_newHostManager(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, hostManager HostManager)
	}{
		{
			name: "new host manager",
			expect: func(t *testing.T, hostManager HostManager) {
				assert := assert.New(t)
				assert.Equal(reflect.TypeOf(hostManager).Elem().Name(), "hostManager")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, newHostManager())
		})
	}
}

func TestHostManager_Load(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, hostManager HostManager, mockHost *Host)
	}{
		{
			name: "load host",
			expect: func(t *testing.T, hostManager HostManager, mockHost *Host) {
				assert := assert.New(t)
				hostManager.Store(mockHost)
				host, ok := hostManager.Load(mockHost.ID)
				assert.Equal(ok, true)
				assert.Equal(host.ID, mockHost.ID)
			},
		},
		{
			name: "host does not exist",
			expect: func(t *testing.T, hostManager HostManager, mockHost *Host) {
				assert := assert.New(t)
				_, ok := hostManager.Load(mockHost.ID)
				assert.Equal(ok, false)
			},
		},
		{
			name: "load key is empty",
			expect: func(t *testing.T, hostManager HostManager, mockHost *Host) {
				assert := assert.New(t)
				mockHost.ID = ""
				hostManager.Store(mockHost)
				host, ok := hostManager.Load(mockHost.ID)
				assert.Equal(ok, true)
				assert.Equal(host.ID, mockHost.ID)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := NewHost(mockRawHost)
			hostManager := newHostManager()
			tc.expect(t, hostManager, mockHost)
		})
	}
}

func TestHostManager_Store(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, hostManager HostManager, mockHost *Host)
	}{
		{
			name: "store host",
			expect: func(t *testing.T, hostManager HostManager, mockHost *Host) {
				assert := assert.New(t)
				hostManager.Store(mockHost)
				host, ok := hostManager.Load(mockHost.ID)
				assert.Equal(ok, true)
				assert.Equal(host.ID, mockHost.ID)
			},
		},
		{
			name: "store key is empty",
			expect: func(t *testing.T, hostManager HostManager, mockHost *Host) {
				assert := assert.New(t)
				mockHost.ID = ""
				hostManager.Store(mockHost)
				host, ok := hostManager.Load(mockHost.ID)
				assert.Equal(ok, true)
				assert.Equal(host.ID, mockHost.ID)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := NewHost(mockRawHost)
			hostManager := newHostManager()
			tc.expect(t, hostManager, mockHost)
		})
	}
}

func TestHostManager_LoadOrStore(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, hostManager HostManager, mockHost *Host)
	}{
		{
			name: "load host exist",
			expect: func(t *testing.T, hostManager HostManager, mockHost *Host) {
				assert := assert.New(t)
				hostManager.Store(mockHost)
				host, ok := hostManager.LoadOrStore(mockHost)
				assert.Equal(ok, true)
				assert.Equal(host.ID, mockHost.ID)
			},
		},
		{
			name: "load host does not exist",
			expect: func(t *testing.T, hostManager HostManager, mockHost *Host) {
				assert := assert.New(t)
				host, ok := hostManager.LoadOrStore(mockHost)
				assert.Equal(ok, false)
				assert.Equal(host.ID, mockHost.ID)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := NewHost(mockRawHost)
			hostManager := newHostManager()
			tc.expect(t, hostManager, mockHost)
		})
	}
}

func TestHostManager_Delete(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, hostManager HostManager, mockHost *Host)
	}{
		{
			name: "delete host",
			expect: func(t *testing.T, hostManager HostManager, mockHost *Host) {
				assert := assert.New(t)
				hostManager.Store(mockHost)
				hostManager.Delete(mockHost.ID)
				_, ok := hostManager.Load(mockHost.ID)
				assert.Equal(ok, false)
			},
		},
		{
			name: "delete key does not exist",
			expect: func(t *testing.T, hostManager HostManager, mockHost *Host) {
				assert := assert.New(t)
				mockHost.ID = ""
				hostManager.Store(mockHost)
				hostManager.Delete(mockHost.ID)
				_, ok := hostManager.Load(mockHost.ID)
				assert.Equal(ok, false)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := NewHost(mockRawHost)
			hostManager := newHostManager()
			tc.expect(t, hostManager, mockHost)
		})
	}
}
