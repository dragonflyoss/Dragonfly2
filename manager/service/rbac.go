package service

import (
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
	if json.Object == "admin" {
		res, err := s.enforcer.AddRoleForUser(json.Subject, "admin")
		if err != nil {
			return err
		}
		if !res {
			logger.Infof("The role %s of %s already exist. skip!", "admin", json.Subject)
		}
		return nil
	}

	wholePermissionRole := rbac.RoleName(json.Object, "write")
	res, err := s.enforcer.HasRoleForUser(json.Subject, wholePermissionRole)
	if err != nil {
		return err
	}
	if !res {
		roleName := rbac.RoleName(json.Object, json.Action)
		res, err := s.enforcer.AddRoleForUser(json.Subject, roleName)
		if err != nil {
			return err
		}
		if !res {
			logger.Infof("The role %s of %s already exist. skip!", roleName, json.Subject)
		}
	} else {
		logger.Infof("The user %s already has whole permission. skip!", json.Subject)
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
			result = append(result, map[string]string{"object": role, "description": role, "action": action})
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
