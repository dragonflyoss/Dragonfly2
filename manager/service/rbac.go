package service

import (
	"errors"
	"strings"

	"d7y.io/dragonfly/v2/manager/types"
	"github.com/gin-gonic/gin"
)

func (s *rest) GetEndpoints(g *gin.Engine) types.Policys {
	policys := []types.Policy{}

	for _, route := range g.Routes() {
		policys = append(policys, types.Policy{
			Method:   route.Method,
			Resource: route.Path,
		})

	}

	return policys

}

func (s *rest) CreatePermission(json types.PolicyRequest) error {
	res := s.enforcer.AddPolicy(json.Subject, json.Object, strings.ToUpper(json.Action))
	if !res {
		return errors.New("failed to add policy")
	}
	return nil
}

func (s *rest) DestroyPermission(json types.PolicyRequest) error {
	res := s.enforcer.RemovePolicy(json.Action, json.Subject, json.Object)
	if !res {
		return errors.New("failed to remove policy")

	}
	return nil
}
