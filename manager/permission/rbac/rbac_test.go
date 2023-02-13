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

package rbac

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetApiGroupName(t *testing.T) {
	tests := []struct {
		name   string
		path   string
		expect func(t *testing.T, data string, err error)
	}{
		{
			name: `path is /api/v1/users`,
			path: "/api/v1/users",
			expect: func(t *testing.T, data string, err error) {
				assert := assert.New(t)
				assert.Equal(data, "users")
			},
		},
		{
			name: `path is /api/v1/users/`,
			path: "/api/v1/users/",
			expect: func(t *testing.T, data string, err error) {
				assert := assert.New(t)
				assert.Equal(data, "users")
			},
		},
		{
			name: `path is /api/v1/users/name`,
			path: "/api/v1/users/name",
			expect: func(t *testing.T, data string, err error) {
				assert := assert.New(t)
				assert.Equal(data, "users")
			},
		},
		{
			name: `path is /api/user`,
			path: "/api/user",
			expect: func(t *testing.T, data string, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "cannot find group name")
			},
		},
		{
			name: "path is empty",
			path: "",
			expect: func(t *testing.T, data string, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "cannot find group name")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			name, err := GetAPIGroupName(tc.path)
			tc.expect(t, name, err)
		})
	}
}

func TestHTTPMethodToAction(t *testing.T) {
	tests := []struct {
		method         string
		expectedAction string
	}{
		{
			method:         "GET",
			expectedAction: ReadAction,
		},
		{
			method:         "POST",
			expectedAction: AllAction,
		},
		{
			method:         "UNKNOWN",
			expectedAction: ReadAction,
		},
	}

	for _, tt := range tests {
		action := HTTPMethodToAction(tt.method)
		if action != tt.expectedAction {
			t.Errorf("HttpMethodToAction(%v) = %v, want %v", tt.method, action, tt.expectedAction)
		}
	}
}
