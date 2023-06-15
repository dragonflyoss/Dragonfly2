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
)

func CORS() gin.HandlerFunc {
	return func(c *gin.Context) {
		origin := c.GetHeader(headers.Origin)
		if origin == "" {
			c.Next()
			return
		}

		c.Header(headers.AccessControlAllowOrigin, origin)
		c.Header(headers.AccessControlAllowCredentials, "true")
		c.Header(headers.AccessControlExposeHeaders, strings.Join([]string{headers.Link, headers.Location, headers.Authorization}, ","))

		if c.Request.Method != http.MethodOptions {
			c.Next()
			return
		}

		// Preflight OPTIONS request.
		c.Header(headers.AccessControlAllowHeaders, c.GetHeader("Access-Control-Request-Headers"))
		c.Header(headers.AccessControlAllowMethods, strings.Join([]string{http.MethodGet, http.MethodHead, http.MethodPut, http.MethodPost, http.MethodDelete, http.MethodPatch}, ","))
		c.Header(headers.AccessControlMaxAge, "600000")
		c.Status(http.StatusNoContent)

		c.Abort()
	}
}
