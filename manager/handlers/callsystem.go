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

// @Summary Create CallSystem
// @Description create by json config
// @Tags CallSystem
// @Accept json
// @Produce json
// @Param CallSystem body types.CreateCallSystemRequest true "CallSystem"
// @Success 200 {object} model.CallSystem
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /callsystem [post]
func (h *Handlers) CreateCallSystem(ctx *gin.Context) {
	var json types.CreateCallSystemRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	schedulerCluster, err := h.service.CreateCallSystem(json)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, schedulerCluster)
}

// @Summary Destroy CallSystem
// @Description Destroy by id
// @Tags CallSystem
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /callsystem/{id} [delete]
func (h *Handlers) DestroyCallSystem(ctx *gin.Context) {
	var params types.CallSystemParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	if err := h.service.DestroyCallSystem(params.ID); err != nil {
		ctx.Error(err)
		return
	}

	ctx.Status(http.StatusOK)
}

// @Summary Update CallSystem
// @Description Update by json config
// @Tags CallSystem
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Param CallSystem body types.UpdateCallSystemRequest true "CallSystem"
// @Success 200 {object} model.CallSystem
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /callsystem/{id} [patch]
func (h *Handlers) UpdateCallSystem(ctx *gin.Context) {
	var params types.CallSystemParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.Error(err)
		return
	}

	var json types.UpdateCallSystemRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.Error(err)
		return
	}

	schedulerCluster, err := h.service.UpdateCallSystem(params.ID, json)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, schedulerCluster)
}

// @Summary Get CallSystem
// @Description Get CallSystem by id
// @Tags CallSystem
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200 {object} model.CallSystem
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /callsystem/{id} [get]
func (h *Handlers) GetCallSystem(ctx *gin.Context) {
	var params types.CallSystemParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	schedulerCluster, err := h.service.GetCallSystem(params.ID)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, schedulerCluster)
}

// @Summary Get CallSystems
// @Description Get CallSystems
// @Tags CallSystem
// @Accept json
// @Produce json
// @Param page query int true "current page" default(0)
// @Param per_page query int true "return max item count, default 10, max 50" default(10) minimum(2) maximum(50)
// @Success 200 {object} []model.CallSystem
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /callsystem [get]
func (h *Handlers) GetCallSystems(ctx *gin.Context) {
	var query types.GetCallSystemsQuery
	if err := ctx.ShouldBindQuery(&query); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	h.setPaginationDefault(&query.Page, &query.PerPage)
	schedulerClusters, err := h.service.GetCallSystems(query)
	if err != nil {
		ctx.Error(err)
		return
	}

	totalCount, err := h.service.CallSystemTotalCount(query)
	if err != nil {
		ctx.Error(err)
		return
	}

	h.setPaginationLinkHeader(ctx, query.Page, query.PerPage, int(totalCount))
	ctx.JSON(http.StatusOK, schedulerClusters)
}
