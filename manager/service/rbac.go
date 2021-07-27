package service

import (
	"errors"

	"d7y.io/dragonfly/v2/manager/permission/rbac"
	"d7y.io/dragonfly/v2/manager/types"
	"github.com/gin-gonic/gin"
)

func (s *rest) GetPermissionGroups(g *gin.Engine) types.PermissionGroups {
	return rbac.SystemRoles(g)
}

func (s *rest) CreatePermission(json types.PolicyRequest) error {
	res, err := s.enforcer.AddRoleForUser(json.Subject, json.Role)
	if err != nil {
		return err
	}
	if !res {
		return errors.New("policy already exists")
	}
	return nil
}

func (s *rest) GetRolesForUser(userName string) ([]string, error) {
	res, err := s.enforcer.GetRolesForUser(userName)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (s *rest) HasRoleForUser(userName, role string) (bool, error) {
	res, err := s.enforcer.HasRoleForUser(userName, role)
	if err != nil {
		return false, err
	}
	return res, nil
}

func (s *rest) DestroyPermission(json types.PolicyRequest) error {
	res, err := s.enforcer.DeleteRoleForUser(json.Subject, json.Role)
	if err != nil {
		return err
	}
	if !res {
		return errors.New("failed to remove policy")

	}
	return nil
}
