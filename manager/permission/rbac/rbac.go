package rbac

import (
	"errors"
	"regexp"
	"strings"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
	"github.com/casbin/casbin/v2"
	"github.com/casbin/casbin/v2/model"
	gormadapter "github.com/casbin/gorm-adapter/v3"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// Syntax for models see https://casbin.org/docs/en/syntax-for-models
const modelText = `
# Request definition
[request_definition]
r = sub, obj, act

# Policy definition
[policy_definition]
p = sub, obj, act, eft

# Role definition
[role_definition]
g = _, _

# Policy effect
[policy_effect]
e = some(where (p.eft == allow))

# Matchers
[matchers]
m = g(r.sub, p.sub) && (r.obj == p.obj) && (r.act == p.act || p.act == '*') || r.sub == "admin"
`

func NewEnforcer(gdb *gorm.DB) (*casbin.Enforcer, error) {
	gormAdapter, err := gormadapter.NewAdapterByDB(gdb)
	if err != nil {
		return nil, err
	}
	m, err := model.NewModelFromString(modelText)
	if err != nil {
		return nil, err
	}
	enforcer, err := casbin.NewEnforcer(m, gormAdapter)
	if err != nil {
		return nil, err
	}
	return enforcer, nil
}

func InitRole(e *casbin.Enforcer, g *gin.Engine) error {
	systemRoles := SystemRoles(g)

	for _, role := range systemRoles {
		roleInfo := strings.Split(role, ":")
		_, err := e.AddPolicy(role, roleInfo[0], roleInfo[1])
		if err != nil {
			return err
		}
	}
	logger.Info("init and check role success")
	return nil

}

func GetAPIGroupName(path string) (string, error) {
	apiGroupRegexp := regexp.MustCompile(`^/api/v[0-9]+/(?P<apiGroup>[\-_a-zA-Z]+)`)
	matchs := apiGroupRegexp.FindStringSubmatch(path)
	if matchs == nil {
		return "", errors.New("faild to find api group")
	}
	apiGroupName := ""
	regexGroupNames := apiGroupRegexp.SubexpNames()
	for i, name := range regexGroupNames {
		if i != 0 && name == "apiGroup" {
			apiGroupName = matchs[i]
		}
	}

	if apiGroupName != "" {
		return apiGroupName, nil
	}
	return "", errors.New("faild to find api group")

}

func SystemRoles(g *gin.Engine) []string {
	Roles := []string{}
	policyKeys := []string{"read", "*"}

	for _, route := range g.Routes() {
		permissionGroupName, err := GetAPIGroupName(route.Path)
		if err != nil {
			continue
		}
		for _, p := range policyKeys {
			if !stringutils.Contains(Roles, permissionGroupName+":"+p) {
				Roles = append(Roles, permissionGroupName+":"+p)
			}

		}
	}
	return Roles
}
