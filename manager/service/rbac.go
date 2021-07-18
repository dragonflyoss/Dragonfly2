package service

import (
	"errors"
	"strings"

	"d7y.io/dragonfly/v2/manager/types"
	"github.com/casbin/casbin"
	"github.com/gin-gonic/gin"
)

var Enforcer *casbin.Enforcer

func (s *rest) GetEndpoints(g *gin.Engine) gin.RoutesInfo {

	return g.Routes()

}

func (s *rest) CreatePermission(json types.PolicyRequest) error {
	res := Enforcer.AddPolicy(json.Subject, json.Object, strings.ToUpper(json.Action))
	if !res {
		return errors.New("failed to add policy")
	}
	return nil
}

func (s *rest) DestroyPermission(json types.PolicyRequest) error {
	res := Enforcer.RemovePolicy(json.Action, json.Subject, json.Object)
	if !res {
		return errors.New("failed to remove policy")

	}
	return nil
}
