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

// @Summary Create Oauth2
// @Description create by json config
// @Tags Oauth2
// @Accept json
// @Produce json
// @Param Oauth2 body types.CreateOauth2Request true "Oauth2"
// @Success 200 {object} model.Oauth2
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /oauth2 [post]
func (h *Handlers) CreateOauth2(ctx *gin.Context) {
	var json types.CreateOauth2Request
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	oauth2, err := h.Service.CreateOauth2(json)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, oauth2)
}

// @Summary Destroy Oauth2
// @Description Destroy by id
// @Tags Oauth2
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /oauth2/{id} [delete]
func (h *Handlers) DestroyOauth2(ctx *gin.Context) {
	var params types.Oauth2Params
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	if err := h.Service.DestroyOauth2(params.ID); err != nil {
		ctx.Error(err)
		return
	}

	ctx.Status(http.StatusOK)
}

// @Summary Update Oauth2
// @Description Update by json config
// @Tags Oauth2
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Param Oauth2 body types.UpdateOauth2Request true "Oauth2"
// @Success 200 {object} model.Oauth2
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /oauth2/{id} [patch]
func (h *Handlers) UpdateOauth2(ctx *gin.Context) {
	var params types.Oauth2Params
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.Error(err)
		return
	}

	var json types.UpdateOauth2Request
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.Error(err)
		return
	}

	oauth2, err := h.Service.UpdateOauth2(params.ID, json)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, oauth2)
}

// @Summary Get Oauth2
// @Description Get Oauth2 by id
// @Tags Oauth2
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200 {object} model.Oauth2
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /oauth2/{id} [get]
func (h *Handlers) GetOauth2(ctx *gin.Context) {
	var params types.Oauth2Params
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	oauth2, err := h.Service.GetOauth2(params.ID)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, oauth2)
}

// @Summary Get Oauth2s
// @Description Get Oauth2s
// @Tags Oauth2
// @Accept json
// @Produce json
// @Param page query int true "current page" default(0)
// @Param per_page query int true "return max item count, default 10, max 50" default(10) minimum(2) maximum(50)
// @Success 200 {object} []model.Oauth2
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /oauth2 [get]
func (h *Handlers) GetOauth2s(ctx *gin.Context) {
	var query types.GetOauth2sQuery
	if err := ctx.ShouldBindQuery(&query); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	h.setPaginationDefault(&query.Page, &query.PerPage)
	oauth2, err := h.Service.GetOauth2s(query)
	if err != nil {
		ctx.Error(err)
		return
	}

	totalCount, err := h.Service.Oauth2TotalCount(query)
	if err != nil {
		ctx.Error(err)
		return
	}

	h.setPaginationLinkHeader(ctx, query.Page, query.PerPage, int(totalCount))
	ctx.JSON(http.StatusOK, oauth2)
}
