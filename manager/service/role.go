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

package service

import (
	"context"

	"d7y.io/dragonfly/v2/manager/types"
)

func (s *service) CreateRole(ctx context.Context, json types.CreateRoleRequest) error {
	for _, permission := range json.Permissions {
		_, err := s.enforcer.AddPermissionForUser(json.Role, permission.Object, permission.Action)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *service) DestroyRole(ctx context.Context, role string) (bool, error) {
	return s.enforcer.DeleteRole(role)
}

func (s *service) GetRole(ctx context.Context, role string) [][]string {
	return s.enforcer.GetPermissionsForUser(role)
}

func (s *service) GetRoles(ctx context.Context) []string {
	return s.enforcer.GetAllSubjects()
}

func (s *service) AddPermissionForRole(ctx context.Context, role string, json types.AddPermissionForRoleRequest) (bool, error) {
	return s.enforcer.AddPermissionForUser(role, json.Object, json.Action)
}

func (s *service) DeletePermissionForRole(ctx context.Context, role string, json types.DeletePermissionForRoleRequest) (bool, error) {
	return s.enforcer.DeletePermissionForUser(role, json.Object, json.Action)
}
