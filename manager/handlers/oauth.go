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

	// nolint
	_ "d7y.io/dragonfly/v2/manager/models"
	"d7y.io/dragonfly/v2/manager/types"
)

// @Summary Create Oauth
// @Description Create by json config
// @Tags Oauth
// @Accept json
// @Produce json
// @Param Oauth body types.CreateOauthRequest true "Oauth"
// @Success 200 {object} models.Oauth
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /oauth [post]
func (h *Handlers) CreateOauth(ctx *gin.Context) {
	var json types.CreateOauthRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	oauth, err := h.service.CreateOauth(ctx.Request.Context(), json)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, oauth)
}

// @Summary Destroy Oauth
// @Description Destroy by id
// @Tags Oauth
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /oauth/{id} [delete]
func (h *Handlers) DestroyOauth(ctx *gin.Context) {
	var params types.OauthParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	if err := h.service.DestroyOauth(ctx.Request.Context(), params.ID); err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.Status(http.StatusOK)
}

// @Summary Update Oauth
// @Description Update by json config
// @Tags Oauth
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Param Oauth body types.UpdateOauthRequest true "Oauth"
// @Success 200 {object} models.Oauth
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /oauth/{id} [patch]
func (h *Handlers) UpdateOauth(ctx *gin.Context) {
	var params types.OauthParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	var json types.UpdateOauthRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	oauth, err := h.service.UpdateOauth(ctx.Request.Context(), params.ID, json)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, oauth)
}

// @Summary Get Oauth
// @Description Get Oauth by id
// @Tags Oauth
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200 {object} models.Oauth
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /oauth/{id} [get]
func (h *Handlers) GetOauth(ctx *gin.Context) {
	var params types.OauthParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	oauth, err := h.service.GetOauth(ctx.Request.Context(), params.ID)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, oauth)
}

// @Summary Get Oauths
// @Description Get Oauths
// @Tags Oauth
// @Accept json
// @Produce json
// @Param page query int true "current page" default(0)
// @Param per_page query int true "return max item count, default 10, max 50" default(10) minimum(2) maximum(50)
// @Success 200 {object} []models.Oauth
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /oauth [get]
func (h *Handlers) GetOauths(ctx *gin.Context) {
	var query types.GetOauthsQuery
	if err := ctx.ShouldBindQuery(&query); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	h.setPaginationDefault(&query.Page, &query.PerPage)
	oauth, count, err := h.service.GetOauths(ctx.Request.Context(), query)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	h.setPaginationLinkHeader(ctx, query.Page, query.PerPage, int(count))
	ctx.JSON(http.StatusOK, oauth)
}
