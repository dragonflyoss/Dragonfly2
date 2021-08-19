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

	"d7y.io/dragonfly/v2/manager/types"
	"github.com/gin-gonic/gin"
)

// @Summary SignUp user
// @Description SignUp user by json config
// @Tags User
// @Accept json
// @Produce json
// @Param User body types.SignUpRequest true "User"
// @Success 200 {object} model.User
// @Failure 400
// @Failure 500
// @Router /user/signup [post]
func (h *Handlers) SignUp(ctx *gin.Context) {
	var json types.SignUpRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	user, err := h.Service.SignUp(json)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, user)
}

// @Summary Revoke Role
// @Description Revoke Role by uri config
// @Tags users
// @Accept json
// @Produce json
// @Success 200
// @Failure 400 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /users/:id/roles/:role_name [delete]

func (h *Handlers) DeleteRoleForUser(ctx *gin.Context) {
	var params types.RoleRequest
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}
	err := h.Service.DeleteRoleForUser(params.ID, params.RoleName)
	if err != nil {
		ctx.Error(err)
		return
	}
	ctx.Status(http.StatusOK)
}

// @Summary Grant Role
// @Description Grant Role by uri config
// @Tags users
// @Accept json
// @Produce json
// @Success 200
// @Failure 400 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /users/:id/roles/:role_name [post]

func (h *Handlers) AddRoleToUser(ctx *gin.Context) {
	var params types.RoleRequest
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}
	err := h.Service.AddRoleForUser(params.ID, params.RoleName)
	if err != nil {
		ctx.Error(err)
		return
	}
	ctx.Status(http.StatusOK)
}

// @Summary Get User Roles
// @Description Get User Roles
// @Tags User
// @Produce json
// @Success 200 {object} RoutesInfo
// @Failure 400 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /users/:id/roles [get]

func (h *Handlers) GetRolesForUser(ctx *gin.Context) {
	var params types.UserParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}
	roles, err := h.Service.GetRolesForUser(params.ID, ctx.GetString("userName"))
	if err != nil {
		ctx.Error(err)
		return
	}
	ctx.JSON(http.StatusOK, roles)

}
