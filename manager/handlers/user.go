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

	jwt "github.com/appleboy/gin-jwt/v2"
	"github.com/gin-gonic/gin"

	// nolint
	_ "d7y.io/dragonfly/v2/manager/models"
	"d7y.io/dragonfly/v2/manager/types"
)

// @Summary Update User
// @Description Update by json config
// @Tags User
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Param User body types.UpdateUserRequest true "User"
// @Success 200 {object} models.User
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /users/{id} [patch]
func (h *Handlers) UpdateUser(ctx *gin.Context) {
	var params types.UserParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	var json types.UpdateUserRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	user, err := h.service.UpdateUser(ctx.Request.Context(), params.ID, json)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, user)
}

// @Summary Get User
// @Description Get User by id
// @Tags User
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200 {object} models.User
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /users/{id} [get]
func (h *Handlers) GetUser(ctx *gin.Context) {
	var params types.UserParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	user, err := h.service.GetUser(ctx.Request.Context(), params.ID)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, user)
}

// @Summary Get Users
// @Description Get Users
// @Tags User
// @Accept json
// @Produce json
// @Param page query int true "current page" default(0)
// @Param per_page query int true "return max item count, default 10, max 50" default(10) minimum(2) maximum(50)
// @Success 200 {object} []models.User
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /users [get]
func (h *Handlers) GetUsers(ctx *gin.Context) {
	var query types.GetUsersQuery
	if err := ctx.ShouldBindQuery(&query); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	h.setPaginationDefault(&query.Page, &query.PerPage)
	users, count, err := h.service.GetUsers(ctx.Request.Context(), query)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	h.setPaginationLinkHeader(ctx, query.Page, query.PerPage, int(count))
	ctx.JSON(http.StatusOK, users)
}

// @Summary SignUp user
// @Description signup by json config
// @Tags User
// @Accept json
// @Produce json
// @Param User body types.SignUpRequest true "User"
// @Success 200 {object} models.User
// @Failure 400
// @Failure 500
// @Router /user/signup [post]
func (h *Handlers) SignUp(ctx *gin.Context) {
	var json types.SignUpRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	user, err := h.service.SignUp(ctx.Request.Context(), json)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, user)
}

// @Summary Reset Password For User
// @Description reset password by json config
// @Tags User
// @Accept json
// @Produce json
// @Param User body types.ResetPasswordRequest true "User"
// @Param id path int true "id"
// @Success 200
// @Failure 400
// @Failure 500
// @Router /users/{id}/reset_password [post]
func (h *Handlers) ResetPassword(ctx *gin.Context) {
	var params types.UserParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	var json types.ResetPasswordRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	if err := h.service.ResetPassword(ctx.Request.Context(), params.ID, json); err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.Status(http.StatusOK)
}

// @Summary Oauth Signin
// @Description oauth signin by json config
// @Tags User
// @Accept json
// @Produce json
// @Param name path string true "name"
// @Success 200
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /user/signin/{name} [get]
func (h *Handlers) OauthSignin(ctx *gin.Context) {
	var params types.OauthSigninParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	authURL, err := h.service.OauthSignin(ctx.Request.Context(), params.Name)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.Redirect(http.StatusFound, authURL)
}

// @Summary Oauth Signin Callback
// @Description oauth signin callback by json config
// @Tags Oauth
// @Param name path string true "name"
// @Param code query string true "code"
// @Success 200
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /user/signin/{name}/callback [get]
func (h *Handlers) OauthSigninCallback(j *jwt.GinJWTMiddleware) func(*gin.Context) {
	return func(ctx *gin.Context) {
		var params types.OauthSigninCallbackParams
		if err := ctx.ShouldBindUri(&params); err != nil {
			ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
			return
		}

		var query types.OauthSigninCallbackQuery
		if err := ctx.ShouldBindQuery(&query); err != nil {
			ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
			return
		}

		user, err := h.service.OauthSigninCallback(ctx.Request.Context(), params.Name, query.Code)
		if err != nil {
			ctx.Error(err) // nolint: errcheck
			return
		}

		ctx.Set("user", user)
		j.LoginHandler(ctx)
	}
}

// @Summary Get User Roles
// @Description get roles by json config
// @Tags User
// @Produce json
// @Param id path string true "id"
// @Success 200 {object} []string
// @Failure 400
// @Failure 500
// @Router /users/{id}/roles [get]
func (h *Handlers) GetRolesForUser(ctx *gin.Context) {
	var params types.UserParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	roles, err := h.service.GetRolesForUser(ctx.Request.Context(), params.ID)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, roles)
}

// @Summary Add Role For User
// @Description add role to user by uri config
// @Tags Users
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Param role path string true "role"
// @Success 200
// @Failure 400
// @Failure 500
// @Router /users/{id}/roles/{role} [put]
func (h *Handlers) AddRoleToUser(ctx *gin.Context) {
	var params types.AddRoleForUserParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	if ok, err := h.service.AddRoleForUser(ctx.Request.Context(), params); err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	} else if !ok {
		ctx.Status(http.StatusConflict)
		return
	}

	ctx.Status(http.StatusOK)
}

// @Summary Delete Role For User
// @Description delete role by uri config
// @Tags Users
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Param role path string true "role"
// @Success 200
// @Failure 400
// @Failure 500
// @Router /users/{id}/roles/{role} [delete]
func (h *Handlers) DeleteRoleForUser(ctx *gin.Context) {
	var params types.DeleteRoleForUserParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	if ok, err := h.service.DeleteRoleForUser(ctx.Request.Context(), params); err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	} else if !ok {
		ctx.Status(http.StatusNotFound)
		return
	}

	ctx.Status(http.StatusOK)
}
