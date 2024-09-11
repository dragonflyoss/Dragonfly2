/*
 *     Copyright 2024 The Dragonfly Authors
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

	"github.com/gin-gonic/gin"

	"d7y.io/dragonfly/v2/pkg/net/ip"
)

const ServerIP = "X-Server-IP"

func Server() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header(ServerIP, fmt.Sprintf("%s, %s", ip.IPv4.String(), ip.IPv6.String()))
		c.Next()
	}
}
