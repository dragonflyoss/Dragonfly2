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

// @Summary Create Config
// @Description Create by json config
// @Tags Config
// @Accept json
// @Produce json
// @Param Config body types.CreateConfigRequest true "Config"
// @Success 200 {object} models.Config
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /configs [post]
func (h *Handlers) CreateConfig(ctx *gin.Context) {
	var json types.CreateConfigRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	config, err := h.service.CreateConfig(ctx.Request.Context(), json)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, config)
}

// @Summary Destroy Config
// @Description Destroy by id
// @Tags Config
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /configs/{id} [delete]
func (h *Handlers) DestroyConfig(ctx *gin.Context) {
	var params types.ConfigParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	if err := h.service.DestroyConfig(ctx.Request.Context(), params.ID); err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.Status(http.StatusOK)
}

// @Summary Update Config
// @Description Update by json config
// @Tags Config
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Param Config body types.UpdateConfigRequest true "Config"
// @Success 200 {object} models.Config
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /configs/{id} [patch]
func (h *Handlers) UpdateConfig(ctx *gin.Context) {
	var params types.ConfigParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	var json types.UpdateConfigRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	config, err := h.service.UpdateConfig(ctx.Request.Context(), params.ID, json)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, config)
}

// @Summary Get Config
// @Description Get Config by id
// @Tags Config
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200 {object} models.Config
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /configs/{id} [get]
func (h *Handlers) GetConfig(ctx *gin.Context) {
	var params types.ConfigParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	config, err := h.service.GetConfig(ctx.Request.Context(), params.ID)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, config)
}

// @Summary Get Configs
// @Description Get Configs
// @Tags Config
// @Accept json
// @Produce json
// @Param page query int true "current page" default(0)
// @Param per_page query int true "return max item count, default 10, max 50" default(10) minimum(2) maximum(50)
// @Success 200 {object} []models.Config
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /configs [get]
func (h *Handlers) GetConfigs(ctx *gin.Context) {
	var query types.GetConfigsQuery
	if err := ctx.ShouldBindQuery(&query); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	h.setPaginationDefault(&query.Page, &query.PerPage)
	configs, count, err := h.service.GetConfigs(ctx.Request.Context(), query)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	h.setPaginationLinkHeader(ctx, query.Page, query.PerPage, int(count))
	ctx.JSON(http.StatusOK, configs)
}
