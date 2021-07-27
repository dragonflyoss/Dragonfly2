package middlewares

import (
	"d7y.io/dragonfly/v2/manager/permission/rbac"
	"github.com/casbin/casbin/v2"
	"github.com/gin-gonic/gin"
)

func RBAC(e *casbin.Enforcer) gin.HandlerFunc {
	return func(c *gin.Context) {
		userName := c.GetString("userName")
		// request path
		p := c.Request.URL.Path
		permissionGroupName, err := rbac.GetApiGroupName(p)
		if err != nil {
			c.Next()
			return
		}
		// request method
		m := c.Request.Method
		action := ""
		switch m {
		case "GET", "HEAD", "OPTIONS":
			action = "read"
		case "POST", "PUT", "PATCH", "DELETE":
			action = "*"
		}
		// rbac validation
		res, err := e.Enforce(userName, permissionGroupName, action)
		if err != nil || !res {
			c.JSON(401, gin.H{
				"message": "permission validate error",
			})
			c.Abort()
			return
		}
		c.Next()

	}
}
