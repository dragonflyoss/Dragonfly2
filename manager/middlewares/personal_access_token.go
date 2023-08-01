/*
 *     Copyright 2023 The Dragonfly Authors
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
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/go-http-utils/headers"
	"gorm.io/gorm"

	"d7y.io/dragonfly/v2/manager/models"
)

func PersonalAccessToken(gdb *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Get bearer token from Authorization header.
		authorization := c.GetHeader(headers.Authorization)
		tokenFields := strings.Fields(authorization)
		if len(tokenFields) != 2 || tokenFields[0] != "Bearer" {
			c.JSON(http.StatusUnauthorized, ErrorResponse{
				Message: http.StatusText(http.StatusUnauthorized),
			})
			c.Abort()
			return
		}

		// Check if the personal access token is valid.
		personalAccessToken := tokenFields[1]
		if err := gdb.WithContext(c).Where("token = ?", personalAccessToken).First(&models.PersonalAccessToken{}).Error; err != nil {
			c.JSON(http.StatusUnauthorized, ErrorResponse{
				Message: http.StatusText(http.StatusUnauthorized),
			})
			c.Abort()
			return
		}

		c.Next()
	}
}
