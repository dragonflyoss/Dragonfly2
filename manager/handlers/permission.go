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

package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// @Summary Get Permissions
// @Description Get Permissions
// @Tags permission
// @Produce json
// @Success 200 {object} RoutesInfo
// @Failure 400 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /permissions [get]
func (h *Handlers) GetPermissions(g *gin.Engine) func(ctx *gin.Context) {
	return func(ctx *gin.Context) {
		permissions := h.Service.GetPermissions(g)
		ctx.JSON(http.StatusOK, permissions)
	}
}
