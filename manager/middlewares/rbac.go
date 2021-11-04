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

package middlewares

import (
	"fmt"
	"net/http"

	"github.com/casbin/casbin/v2"
	"github.com/gin-gonic/gin"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/manager/permission/rbac"
)

func RBAC(e *casbin.Enforcer) gin.HandlerFunc {
	return func(c *gin.Context) {
		action := rbac.HTTPMethodToAction(c.Request.Method)
		permission, err := rbac.GetAPIGroupName(c.Request.URL.Path)
		if err != nil {
			logger.Errorf("get api group name error: %s", err)
			c.JSON(http.StatusUnauthorized, gin.H{
				"message": "permission validate error!",
			})
			c.Abort()
			return
		}

		id, ok := c.Get("id")
		if !ok {
			c.JSON(http.StatusUnauthorized, gin.H{
				"message": "permission validate error!",
			})
			c.Abort()
			return
		}

		if ok, err := e.Enforce(fmt.Sprint(id.(float64)), permission, action); err != nil {
			logger.Errorf("RBAC validate error: %s", err)
			c.JSON(http.StatusUnauthorized, gin.H{
				"message": "permission validate error!",
			})
			c.Abort()
			return
		} else if !ok {
			c.JSON(http.StatusUnauthorized, gin.H{
				"message": "permission deny",
			})
			c.Abort()
			return
		}

		c.Next()
	}
}
