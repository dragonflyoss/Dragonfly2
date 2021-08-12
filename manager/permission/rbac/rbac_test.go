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
)

func TestGetApiGroupName(t *testing.T) {
	tests := []struct {
		path              string
		exceptedGroupName string
		hasError          bool
	}{
		{
			path:              "/api/v1/users",
			exceptedGroupName: "users",
			hasError:          false,
		},
		{
			path:              "/api/user",
			exceptedGroupName: "",
			hasError:          true,
		},
	}

	for _, tt := range tests {
		groupName, err := GetAPIGroupName(tt.path)
		if tt.hasError {
			if err == nil {
				t.Errorf("GetApiGroupName(%s) should return error", tt.path)
			}
		}

		if groupName != tt.exceptedGroupName {
			t.Errorf("GetApiGroupName(%v) = %v, want %v", tt.path, groupName, tt.exceptedGroupName)
		}
	}

}

func TestRoleName(t *testing.T) {
	tests := []struct {
		object           string
		action           string
		exceptedRoleName string
	}{
		{
			object:           "users",
			action:           "read",
			exceptedRoleName: "users:read",
		},
		{
			object:           "cdns",
			action:           "write",
			exceptedRoleName: "cdns:*",
		},
	}

	for _, tt := range tests {
		roleName := RoleName(tt.object, tt.action)
		if roleName != tt.exceptedRoleName {
			t.Errorf("RoleName(%v, %v) = %v, want %v", tt.object, tt.action, roleName, tt.exceptedRoleName)
		}
	}

}
func TestHTTPMethodToAction(t *testing.T) {
	tests := []struct {
		method         string
		exceptedAction string
	}{
		{
			method:         "GET",
			exceptedAction: "read",
		},
		{
			method:         "POST",
			exceptedAction: "*",
		},
		{
			method:         "UNKNOWN",
			exceptedAction: "read",
		},
	}

	for _, tt := range tests {
		action := HTTPMethodToAction(tt.method)
		if action != tt.exceptedAction {
			t.Errorf("HttpMethodToAction(%v) = %v, want %v", tt.method, action, tt.exceptedAction)
		}
	}

}
