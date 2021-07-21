package rbac

import (
	"github.com/casbin/casbin"
	gormadapter "github.com/casbin/gorm-adapter/v3"
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
m = g(r.sub, p.sub) && keyMatch2(r.obj, p.obj) && regexMatch(r.act == p.act || p.act == '*') || r.sub == "admin"
`

func NewEnforcer(gdb *gorm.DB) (*casbin.Enforcer, error) {
	gormAdapter, err := gormadapter.NewAdapterByDB(gdb)
	if err != nil {
		return nil, err
	}
	enforcer := casbin.NewEnforcer(modelText, gormAdapter, true)
	return enforcer, nil
}
