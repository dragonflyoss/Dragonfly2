package service

import (
	"errors"

	"d7y.io/dragonfly/v2/manager/permission/rbac"
	"d7y.io/dragonfly/v2/manager/types"
	"github.com/gin-gonic/gin"
)

func (s *rest) GetPermissionGroups(g *gin.Engine) types.PermissionGroups {
	return rbac.GetAPIGroupNames(g)
}

func (s *rest) CreatePermission(json types.PolicyRequest) error {
	roleName := rbac.RoleName(json.Object, json.Action)
	res, err := s.enforcer.AddRoleForUser(json.Subject, roleName)
	if err != nil {
		return err
	}
	if !res {
		return errors.New("policy already exists")
	}
	return nil
}

func (s *rest) GetRolesForUser(subject string) ([]string, error) {
	res, err := s.enforcer.GetRolesForUser(subject)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (s *rest) HasRoleForUser(subject, object, action string) (bool, error) {
	roleName := rbac.RoleName(object, action)
	res, err := s.enforcer.HasRoleForUser(subject, roleName)
	if err != nil {
		return false, err
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
		return errors.New("failed to remove policy")

	}
	return nil
}
