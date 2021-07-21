package middlewares

import (
	"github.com/casbin/casbin"
	"github.com/gin-gonic/gin"
)

func RBAC(e *casbin.Enforcer) gin.HandlerFunc {
	return func(c *gin.Context) {
		userName := c.GetString("userName")
		// request path
		p := c.Request.URL.Path
		// request method
		m := c.Request.Method
		// rbac validation
		res, err := e.EnforceSafe(userName, p, m)
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
