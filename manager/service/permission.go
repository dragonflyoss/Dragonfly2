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
	"errors"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/manager/permission/rbac"
	"d7y.io/dragonfly/v2/manager/types"
	"github.com/gin-gonic/gin"
)

func (s *rest) GetPermissions(g *gin.Engine) types.Permissions {
	return rbac.GetAPIGroupNames(g)
}

func (s *rest) CreateRole(json types.RolePermissionCreateRequest) error {
	for _, p := range json.Permissions {
		res, err := s.enforcer.AddPolicy(json.RoleName, p.Object, p.Action)
		s.enforcer.GetAllObjects()
		if err != nil {
			return err
		}
		if !res {
			logger.Infof("The role %s that %s for %s already exist. skip!", json.RoleName, p.Object, p.Action)
		}

	}
	return nil
}

func (s *rest) GetRoles() []string {
	return s.enforcer.GetAllSubjects()
}

func (s *rest) UpdateRole(roleName string, json types.RolePermissionUpdateRequest) error {
	switch json.Method {
	case "add":
		for _, p := range json.Permissions {
			_, err := s.enforcer.AddPolicy(roleName, p.Object, p.Action)
			if err != nil {
				return err
			}

		}
	case "remove":
		for _, p := range json.Permissions {
			_, err := s.enforcer.RemovePolicy(roleName, p.Object, p.Action)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *rest) DestroyRole(roleName string) error {
	_, err := s.enforcer.DeleteRole(roleName)
	s.enforcer.GetAllRoles()
	if err != nil {
		return err
	}
	return nil
}

func (s *rest) GetRole(roleName string) []map[string]string {
	result := []map[string]string{}
	policies := s.enforcer.GetFilteredPolicy(0, roleName)
	for _, p := range policies {
		result = append(result, map[string]string{"object": p[1], "action": p[2]})
	}
	return result
}

func (s *rest) GetRolesForUser(userName, currentUserName string) ([]string, error) {
	var results []string
	var err error
	if userName == currentUserName {
		results, err = s.enforcer.GetRolesForUser(userName)
		if err != nil {
			return nil, err
		}
	} else {
		has, err := s.enforcer.Enforce(currentUserName, "users", "read")
		if err != nil {
			return nil, err
		}
		if has {
			results, err = s.enforcer.GetRolesForUser(userName)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, errors.New("permission deny")
		}
	}

	return results, nil
}

func (s *rest) HasRoleForUser(subject, object, action string) (bool, error) {
	roleName := rbac.RoleName(object, action)
	res, err := s.enforcer.HasRoleForUser(subject, roleName)
	if err != nil {
		return false, err
	}
	return res, nil
}
