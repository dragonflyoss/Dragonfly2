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
	"fmt"
	"strings"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/manager/permission/rbac"
	"d7y.io/dragonfly/v2/manager/types"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
	"github.com/gin-gonic/gin"
)

func (s *rest) GetPermissionGroups(g *gin.Engine) types.PermissionGroups {
	groups := rbac.GetAPIGroupNames(g)
	if !stringutils.Contains(groups, "admin") {
		groups = append(groups, "admin")
	}
	return groups
}

func (s *rest) CreatePermission(json types.PolicyRequest) error {
	roleName := rbac.RoleName(json.Object, json.Action)
	res, err := s.enforcer.AddRoleForUser(json.Subject, roleName)
	if err != nil {
		return err
	}
	if !res {
		logger.Infof("The role %s of %s already exist. skip!", roleName, json.Subject)
	}
	return nil
}

func (s *rest) GetRolesForUser(subject string) ([]map[string]string, error) {
	result := []map[string]string{}
	policyToAction := map[string]string{
		"read": "read",
		"*":    "write",
	}
	res, err := s.enforcer.GetRolesForUser(subject)
	if err != nil {
		return nil, err
	}

	for _, role := range res {
		if role == "admin" {
			result = append(result, map[string]string{"object": "admin", "description": "admin", "action": ""})
		} else {
			roleInfo := strings.Split(role, ":")
			action := policyToAction[roleInfo[1]]
			result = append(result, map[string]string{"object": roleInfo[0], "description": fmt.Sprintf("%s for %s", action, roleInfo[0]), "action": action})
		}
	}

	return result, nil
}

func (s *rest) HasRoleForUser(subject, object, action string) (bool, error) {
	roleName := rbac.RoleName(object, action)
	res, err := s.enforcer.HasRoleForUser(subject, roleName)
	if err != nil {
		return false, err
	}
	if action == "read" {
		writeRoleName := rbac.RoleName(object, "write")
		writeRes, err := s.enforcer.HasRoleForUser(subject, writeRoleName)
		if err != nil {
			return false, err
		}
		return res || writeRes, nil

	}
	return res, nil
}

func (s *rest) DestroyPermission(json types.PolicyRequest) error {
	roleName := rbac.RoleName(json.Object, json.Action)
	res, err := s.enforcer.DeleteRoleForUser(json.Subject, roleName)
	if err != nil {
		return err
	}
	if !res {
		logger.Infof("The role %s of %s already remove. skip!", roleName, json.Subject)
	}
	return nil
}
