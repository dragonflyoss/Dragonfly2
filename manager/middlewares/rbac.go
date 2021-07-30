package middlewares

import (
	"net/http"

	"d7y.io/dragonfly/v2/manager/permission/rbac"
	"github.com/casbin/casbin/v2"
	"github.com/gin-gonic/gin"
)

func RBAC(e *casbin.Enforcer) gin.HandlerFunc {
	return func(c *gin.Context) {
		userName := c.GetString("userName")
		// request path
		p := c.Request.URL.Path
		permissionGroupName, err := rbac.GetAPIGroupName(p)
		if err != nil {
			c.Next()
			return
		}
		// request method
		m := c.Request.Method
		action := rbac.HTTPMethodToAction(m)
		// rbac validation
		adminRes, err := e.HasRoleForUser(userName, "admin")
		if err != nil {
			c.JSON(http.StatusUnauthorized, gin.H{
				"message": "permission validate error",
			})
			c.Abort()
			return
		}
		if adminRes {
			c.Next()
			return
		}
		res, err := e.Enforce(userName, permissionGroupName, action)
		if err != nil || !res {
			c.JSON(http.StatusUnauthorized, gin.H{
				"message": "permission validate error",
			})
			c.Abort()
			return
		}
		c.Next()

	}
}
