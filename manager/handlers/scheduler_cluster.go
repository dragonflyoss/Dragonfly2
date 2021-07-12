package handlers

import (
	"net/http"

	"d7y.io/dragonfly.v2/manager/types"
	"github.com/gin-gonic/gin"
)

// @Summary Create SchedulerCluster
// @Description create by json config
// @Tags SchedulerCluster
// @Accept json
// @Produce json
// @Param SchedulerCluster body types.CreateSchedulerClusterRequest true "SchedulerCluster"
// @Success 200 {object} model.SchedulerCluster
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /scheduler-clusters [post]
func (h *Handlers) CreateSchedulerCluster(ctx *gin.Context) {
	var json types.CreateSchedulerClusterRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	if json.SecurityGroupDomain != "" {
		scheduler, err := h.Service.CreateSchedulerClusterWithSecurityGroupDomain(json)
		if err != nil {
			ctx.Error(err)
			return
		}

		ctx.JSON(http.StatusOK, scheduler)
		return
	}

	schedulerCluster, err := h.Service.CreateSchedulerCluster(json)
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
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /scheduler-clusters/{id} [delete]
func (h *Handlers) DestroySchedulerCluster(ctx *gin.Context) {
	var params types.SchedulerClusterParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	err := h.Service.DestroySchedulerCluster(params.ID)
	if err != nil {
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
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
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
		scheduler, err := h.Service.UpdateSchedulerClusterWithSecurityGroupDomain(params.ID, json)
		if err != nil {
			ctx.Error(err)
			return
		}

		ctx.JSON(http.StatusOK, scheduler)
		return
	}

	schedulerCluster, err := h.Service.UpdateSchedulerCluster(params.ID, json)
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
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /scheduler-clusters/{id} [get]
func (h *Handlers) GetSchedulerCluster(ctx *gin.Context) {
	var params types.SchedulerClusterParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	schedulerCluster, err := h.Service.GetSchedulerCluster(params.ID)
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
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /scheduler-clusters [get]
func (h *Handlers) GetSchedulerClusters(ctx *gin.Context) {
	var query types.GetSchedulerClustersQuery
	if err := ctx.ShouldBindQuery(&query); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	h.setPaginationDefault(&query.Page, &query.PerPage)
	schedulerClusters, err := h.Service.GetSchedulerClusters(query)
	if err != nil {
		ctx.Error(err)
		return
	}

	totalCount, err := h.Service.SchedulerClusterTotalCount(query)
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
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /scheduler-clusters/{id}/schedulers/{scheduler_id} [put]
func (h *Handlers) AddSchedulerToSchedulerCluster(ctx *gin.Context) {
	var params types.AddSchedulerToSchedulerClusterParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	err := h.Service.AddSchedulerToSchedulerCluster(params.ID, params.SchedulerID)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.Status(http.StatusOK)
}
