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
	"d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/manager/permission/rbac"
	"d7y.io/dragonfly/v2/manager/types"
	"github.com/gin-gonic/gin"
)

func (s *rest) GetPermissions(g *gin.Engine) types.Permissions {
	return rbac.GetAPIGroupNames(g)
}

func (s *rest) CreateRole(json types.CreateRolePermissionRequest) error {
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

func (s *rest) AddRolePermission(roleName string, json types.ObjectPermission) error {
	_, err := s.enforcer.AddPolicy(roleName, json.Object, json.Action)
	if err != nil {
		return err
	}

	return nil
}

func (s *rest) RemoveRolePermission(roleName string, json types.ObjectPermission) error {
	_, err := s.enforcer.RemovePolicy(roleName, json.Object, json.Action)
	if err != nil {
		return err
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

func (s *rest) GetRolesForUser(UserID uint, currentUserName string) ([]string, error) {
	var results []string
	var err error
	user := model.User{}
	if err := s.db.First(&user, UserID).Error; err != nil {
		return nil, err
	}
	queryUserName := user.Name

	if queryUserName == currentUserName {
		results, err = s.enforcer.GetRolesForUser(queryUserName)
		if err != nil {
			return nil, err
		}
	} else {
		has, err := s.enforcer.Enforce(currentUserName, "users", "read")
		if err != nil {
			return nil, err
		}
		if has {
			results, err = s.enforcer.GetRolesForUser(queryUserName)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, errors.New("permission deny")
		}
	}

	return results, nil
}
