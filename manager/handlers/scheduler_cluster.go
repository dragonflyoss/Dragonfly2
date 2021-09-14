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

// @Summary Create SchedulerCluster
// @Description create by json config
// @Tags SchedulerCluster
// @Accept json
// @Produce json
// @Param SchedulerCluster body types.CreateSchedulerClusterRequest true "SchedulerCluster"
// @Success 200 {object} model.SchedulerCluster
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /scheduler-clusters [post]
func (h *Handlers) CreateSchedulerCluster(ctx *gin.Context) {
	var json types.CreateSchedulerClusterRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	if json.SecurityGroupDomain != "" {
		scheduler, err := h.service.CreateSchedulerClusterWithSecurityGroupDomain(json)
		if err != nil {
			ctx.Error(err)
			return
		}

		ctx.JSON(http.StatusOK, scheduler)
		return
	}

	schedulerCluster, err := h.service.CreateSchedulerCluster(json)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, schedulerCluster)
}

// @Summary Destroy SchedulerCluster
// @Description Destroy by id
// @Tags SchedulerCluster
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /scheduler-clusters/{id} [delete]
func (h *Handlers) DestroySchedulerCluster(ctx *gin.Context) {
	var params types.SchedulerClusterParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	if err := h.service.DestroySchedulerCluster(params.ID); err != nil {
		ctx.Error(err)
		return
	}

	ctx.Status(http.StatusOK)
}

// @Summary Update SchedulerCluster
// @Description Update by json config
// @Tags SchedulerCluster
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Param SchedulerCluster body types.UpdateSchedulerClusterRequest true "SchedulerCluster"
// @Success 200 {object} model.SchedulerCluster
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /scheduler-clusters/{id} [patch]
func (h *Handlers) UpdateSchedulerCluster(ctx *gin.Context) {
	var params types.SchedulerClusterParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.Error(err)
		return
	}

	var json types.UpdateSchedulerClusterRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.Error(err)
		return
	}

	if json.SecurityGroupDomain != "" {
		scheduler, err := h.service.UpdateSchedulerClusterWithSecurityGroupDomain(params.ID, json)
		if err != nil {
			ctx.Error(err)
			return
		}

		ctx.JSON(http.StatusOK, scheduler)
		return
	}

	schedulerCluster, err := h.service.UpdateSchedulerCluster(params.ID, json)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, schedulerCluster)
}

// @Summary Get SchedulerCluster
// @Description Get SchedulerCluster by id
// @Tags SchedulerCluster
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200 {object} model.SchedulerCluster
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /scheduler-clusters/{id} [get]
func (h *Handlers) GetSchedulerCluster(ctx *gin.Context) {
	var params types.SchedulerClusterParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	schedulerCluster, err := h.service.GetSchedulerCluster(params.ID)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, schedulerCluster)
}

// @Summary Get SchedulerClusters
// @Description Get SchedulerClusters
// @Tags SchedulerCluster
// @Accept json
// @Produce json
// @Param page query int true "current page" default(0)
// @Param per_page query int true "return max item count, default 10, max 50" default(10) minimum(2) maximum(50)
// @Success 200 {object} []model.SchedulerCluster
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /scheduler-clusters [get]
func (h *Handlers) GetSchedulerClusters(ctx *gin.Context) {
	var query types.GetSchedulerClustersQuery
	if err := ctx.ShouldBindQuery(&query); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	h.setPaginationDefault(&query.Page, &query.PerPage)
	schedulerClusters, err := h.service.GetSchedulerClusters(query)
	if err != nil {
		ctx.Error(err)
		return
	}

	totalCount, err := h.service.SchedulerClusterTotalCount(query)
	if err != nil {
		ctx.Error(err)
		return
	}

	h.setPaginationLinkHeader(ctx, query.Page, query.PerPage, int(totalCount))
	ctx.JSON(http.StatusOK, schedulerClusters)
}

// @Summary Add Scheduler to schedulerCluster
// @Description Add Scheduler to schedulerCluster
// @Tags SchedulerCluster
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Param scheduler_id path string true "scheduler id"
// @Success 200
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /scheduler-clusters/{id}/schedulers/{scheduler_id} [put]
func (h *Handlers) AddSchedulerToSchedulerCluster(ctx *gin.Context) {
	var params types.AddSchedulerToSchedulerClusterParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	err := h.service.AddSchedulerToSchedulerCluster(params.ID, params.SchedulerID)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.Status(http.StatusOK)
}
