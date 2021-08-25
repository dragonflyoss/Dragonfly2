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

package rbac

import (
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	managermodel "d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
	"github.com/casbin/casbin/v2"
	"github.com/casbin/casbin/v2/model"
	gormadapter "github.com/casbin/gorm-adapter/v3"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// Syntax for models see https://casbin.org/docs/en/syntax-for-models
const modelText = `
[request_definition]
r = sub, obj, act

[policy_definition]
p = sub, obj, act

[role_definition]
g = _, _

[policy_effect]
e = some(where (p.eft == allow))

[matchers]
m = g(r.sub, p.sub) && r.obj == p.obj && (r.act == p.act || p.act == "*")
`

func NewEnforcer(gdb *gorm.DB) (*casbin.Enforcer, error) {
	adapter, err := gormadapter.NewAdapterByDBWithCustomTable(gdb, &managermodel.CasbinRule{})
	if err != nil {
		return nil, err
	}
	m, err := model.NewModelFromString(modelText)
	if err != nil {
		return nil, err
	}
	enforcer, err := casbin.NewEnforcer(m, adapter)
	if err != nil {
		return nil, err
	}
	return enforcer, nil
}

func InitRole(e *casbin.Enforcer, g *gin.Engine) error {
	systemRoles := SystemRoles(g)

	var err error
	for _, role := range systemRoles {
		roleInfo := strings.Split(role, ":")
		_, err = e.AddPolicy(role, roleInfo[0], roleInfo[1])
		if err != nil {
			return err
		}

		// init admin permissions
		_, err = e.AddPolicy("admin", roleInfo[0], "*")
		if err != nil {
			return err
		}
	}
	if _, err := e.AddRoleForUser("admin", "admin"); err != nil {
		return err
	}
	logger.Info("init and check role success")
	return nil

}

func GetAPIGroupName(path string) (string, error) {
	apiGroupRegexp := regexp.MustCompile(`^/api/v[0-9]+/([-_a-zA-Z]*)[/.*]*`)
	matchs := apiGroupRegexp.FindStringSubmatch(path)
	if len(matchs) != 2 {
		return "", errors.New("faild to find api group")
	}

	return matchs[1], nil
}

func RoleName(object, action string) string {
	if object == "admin" {
		return "admin"
	}
	return fmt.Sprintf("%s:%s", object, action)
}

func GetAPIGroupNames(g *gin.Engine) []string {
	APIGroups := []string{}
	for _, route := range g.Routes() {
		apiGroupName, err := GetAPIGroupName(route.Path)
		if err != nil {
			continue
		}
		if !stringutils.Contains(APIGroups, apiGroupName) {
			APIGroups = append(APIGroups, apiGroupName)
		}

	}

	return APIGroups
}

func SystemRoles(g *gin.Engine) []string {
	Roles := []string{}
	policyKeys := []string{"read", "*"}

	for _, apiGroup := range GetAPIGroupNames(g) {
		for _, p := range policyKeys {
			if !stringutils.Contains(Roles, apiGroup+":"+p) {
				Roles = append(Roles, apiGroup+":"+p)
			}

		}
	}
	return Roles
}

func HTTPMethodToAction(method string) string {
	action := "read"

	if method == http.MethodDelete || method == http.MethodPatch || method == http.MethodPut || method == http.MethodPost {
		action = "*"
	}

	return action
}
