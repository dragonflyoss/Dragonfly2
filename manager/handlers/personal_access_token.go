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

package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"

	// nolint
	_ "d7y.io/dragonfly/v2/manager/models"
	"d7y.io/dragonfly/v2/manager/types"
)

// @Summary Create PersonalAccessToken
// @Description Create by json config
// @Tags PersonalAccessToken
// @Accept json
// @Produce json
// @Param PersonalAccessToken body types.CreatePersonalAccessTokenRequest true "PersonalAccessToken"
// @Success 200 {object} models.PersonalAccessToken
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /personal-access-tokens [post]
func (h *Handlers) CreatePersonalAccessToken(ctx *gin.Context) {
	var json types.CreatePersonalAccessTokenRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	personalAccessToken, err := h.service.CreatePersonalAccessToken(ctx.Request.Context(), json)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, personalAccessToken)
}

// @Summary Destroy PersonalAccessToken
// @Description Destroy by id
// @Tags PersonalAccessToken
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /personal-access-tokens/{id} [delete]
func (h *Handlers) DestroyPersonalAccessToken(ctx *gin.Context) {
	var params types.PersonalAccessTokenParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	if err := h.service.DestroyPersonalAccessToken(ctx.Request.Context(), params.ID); err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.Status(http.StatusOK)
}

// @Summary Update PersonalAccessToken
// @Description Update by json config
// @Tags PersonalAccessToken
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Param PersonalAccessToken body types.UpdatePersonalAccessTokenRequest true "PersonalAccessToken"
// @Success 200 {object} models.PersonalAccessToken
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /personal-access-tokens/{id} [patch]
func (h *Handlers) UpdatePersonalAccessToken(ctx *gin.Context) {
	var params types.PersonalAccessTokenParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	var json types.UpdatePersonalAccessTokenRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	personalAccessToken, err := h.service.UpdatePersonalAccessToken(ctx.Request.Context(), params.ID, json)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, personalAccessToken)
}

// @Summary Get PersonalAccessToken
// @Description Get PersonalAccessToken by id
// @Tags PersonalAccessToken
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200 {object} models.PersonalAccessToken
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /personal-access-tokens/{id} [get]
func (h *Handlers) GetPersonalAccessToken(ctx *gin.Context) {
	var params types.PersonalAccessTokenParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	personalAccessToken, err := h.service.GetPersonalAccessToken(ctx.Request.Context(), params.ID)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, personalAccessToken)
}

// @Summary Get PersonalAccessTokens
// @Description Get PersonalAccessTokens
// @Tags PersonalAccessToken
// @Accept json
// @Produce json
// @Param page query int true "current page" default(0)
// @Param per_page query int true "return max item count, default 10, max 50" default(10) minimum(2) maximum(50)
// @Success 200 {object} []models.PersonalAccessToken
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /personal-access-tokens [get]
func (h *Handlers) GetPersonalAccessTokens(ctx *gin.Context) {
	var query types.GetPersonalAccessTokensQuery
	if err := ctx.ShouldBindQuery(&query); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	h.setPaginationDefault(&query.Page, &query.PerPage)
	personalAccessTokens, count, err := h.service.GetPersonalAccessTokens(ctx.Request.Context(), query)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	h.setPaginationLinkHeader(ctx, query.Page, query.PerPage, int(count))
	ctx.JSON(http.StatusOK, personalAccessTokens)
}
