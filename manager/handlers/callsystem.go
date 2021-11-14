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
	_ "d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/manager/types"
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

	schedulerCluster, err := h.service.CreateCallSystem(ctx.Request.Context(), json)
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

	if err := h.service.DestroyCallSystem(ctx.Request.Context(), params.ID); err != nil {
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
// @Router /callsystems/{id} [patch]
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

	schedulerCluster, err := h.service.UpdateCallSystem(ctx.Request.Context(), params.ID, json)
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
// @Router /callsystems/{id} [get]
func (h *Handlers) GetCallSystem(ctx *gin.Context) {
	var params types.CallSystemParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	schedulerCluster, err := h.service.GetCallSystem(ctx.Request.Context(), params.ID)
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
// @Router /callsystems [get]
func (h *Handlers) GetCallSystems(ctx *gin.Context) {
	var query types.GetCallSystemsQuery
	if err := ctx.ShouldBindQuery(&query); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	h.setPaginationDefault(&query.Page, &query.PerPage)
	schedulerClusters, count, err := h.service.GetCallSystems(ctx.Request.Context(), query)
	if err != nil {
		ctx.Error(err)
		return
	}

	h.setPaginationLinkHeader(ctx, query.Page, query.PerPage, int(count))
	ctx.JSON(http.StatusOK, schedulerClusters)
}

// @Summary Add Scheduler to CallSystem
// @Description Add Scheduler to CallSystem
// @Tags CallSystem
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Param scheduler_cluster_id path string true "scheduler cluster id"
// @Success 200
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /callsystems/{id}/scheduler-clusters/{scheduler_cluster_id} [put]
func (h *Handlers) AddSchedulerClusterToCallSystem(ctx *gin.Context) {
	var params types.AddSchedulerClusterToCallSystemParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	if err := h.service.AddSchedulerClusterToCallSystem(ctx.Request.Context(), params.ID, params.SchedulerClusterID); err != nil {
		ctx.Error(err)
		return
	}

	ctx.Status(http.StatusOK)
}

// @Summary Delete Scheduler to CallSystem
// @Description Delete Scheduler to CallSystem
// @Tags CallSystem
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Param scheduler_cluster_id path string true "scheduler cluster id"
// @Success 200
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /callsystems/{id}/scheduler-clusters/{scheduler_cluster_id} [delete]
func (h *Handlers) DeleteSchedulerClusterToCallSystem(ctx *gin.Context) {
	var params types.DeleteSchedulerClusterToCallSystemParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	if err := h.service.DeleteSchedulerClusterToCallSystem(ctx.Request.Context(), params.ID, params.SchedulerClusterID); err != nil {
		ctx.Error(err)
		return
	}

	ctx.Status(http.StatusOK)
}

// @Summary Add CDN to CallSystem
// @Description Add CDN to CallSystem
// @Tags CallSystem
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Param cdn_cluster_id path string true "cdn cluster id"
// @Success 200
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /callsystems/{id}/cdn-clusters/{cdn_cluster_id} [put]
func (h *Handlers) AddCDNClusterToCallSystem(ctx *gin.Context) {
	var params types.AddCDNClusterToCallSystemParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	if err := h.service.AddCDNClusterToCallSystem(ctx.Request.Context(), params.ID, params.CDNClusterID); err != nil {
		ctx.Error(err)
		return
	}

	ctx.Status(http.StatusOK)
}

// @Summary Delete CDN to CallSystem
// @Description Delete CDN to CallSystem
// @Tags CallSystem
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Param cdn_cluster_id path string true "cdn cluster id"
// @Success 200
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /callsystems/{id}/cdn-clusters/{cdn_cluster_id} [delete]
func (h *Handlers) DeleteCDNClusterToCallSystem(ctx *gin.Context) {
	var params types.DeleteCDNClusterToCallSystemParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	if err := h.service.DeleteCDNClusterToCallSystem(ctx.Request.Context(), params.ID, params.CDNClusterID); err != nil {
		ctx.Error(err)
		return
	}

	ctx.Status(http.StatusOK)
}
