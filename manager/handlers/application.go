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

// @Summary Create Application
// @Description Create by json config
// @Tags Application
// @Accept json
// @Produce json
// @Param Application body types.CreateApplicationRequest true "Application"
// @Success 200 {object} models.Application
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /applications [post]
func (h *Handlers) CreateApplication(ctx *gin.Context) {
	var json types.CreateApplicationRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	application, err := h.service.CreateApplication(ctx.Request.Context(), json)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, application)
}

// @Summary Destroy Application
// @Description Destroy by id
// @Tags Application
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /applications/{id} [delete]
func (h *Handlers) DestroyApplication(ctx *gin.Context) {
	var params types.ApplicationParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	if err := h.service.DestroyApplication(ctx.Request.Context(), params.ID); err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.Status(http.StatusOK)
}

// @Summary Update Application
// @Description Update by json config
// @Tags Application
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Param Application body types.UpdateApplicationRequest true "Application"
// @Success 200 {object} models.Application
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /applications/{id} [patch]
func (h *Handlers) UpdateApplication(ctx *gin.Context) {
	var params types.ApplicationParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	var json types.UpdateApplicationRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	application, err := h.service.UpdateApplication(ctx.Request.Context(), params.ID, json)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, application)
}

// @Summary Get Application
// @Description Get Application by id
// @Tags Application
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200 {object} models.Application
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /applications/{id} [get]
func (h *Handlers) GetApplication(ctx *gin.Context) {
	var params types.ApplicationParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	application, err := h.service.GetApplication(ctx.Request.Context(), params.ID)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, application)
}

// @Summary Get Applications
// @Description Get Applications
// @Tags Application
// @Accept json
// @Produce json
// @Param page query int true "current page" default(0)
// @Param per_page query int true "return max item count, default 10, max 50" default(10) minimum(2) maximum(50)
// @Success 200 {object} []models.Application
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /applications [get]
func (h *Handlers) GetApplications(ctx *gin.Context) {
	var query types.GetApplicationsQuery
	if err := ctx.ShouldBindQuery(&query); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	h.setPaginationDefault(&query.Page, &query.PerPage)
	applications, count, err := h.service.GetApplications(ctx.Request.Context(), query)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	h.setPaginationLinkHeader(ctx, query.Page, query.PerPage, int(count))
	ctx.JSON(http.StatusOK, applications)
}
