package handlers

import (
	"net/http"

	"d7y.io/dragonfly/v2/manager/types"
	"github.com/gin-gonic/gin"
)

// @Summary Create SchedulerInstance
// @Description create by json config
// @Tags SchedulerInstance
// @Accept json
// @Produce json
// @Param SchedulerInstance body types.CreateSchedulerInstanceRequest true "SchedulerInstance"
// @Success 200 {object} model.SchedulerInstance
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /scheduler-instances [post]
func (h *Handlers) CreateSchedulerInstance(ctx *gin.Context) {
	var json types.CreateSchedulerInstanceRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	if json.SecurityGroupDomain != "" {
		schedulerInstance, err := h.service.CreateSchedulerInstanceWithSecurityGroupDomain(json)
		if err != nil {
			ctx.Error(err)
			return
		}

		ctx.JSON(http.StatusOK, schedulerInstance)
		return
	}

	schedulerInstance, err := h.service.CreateSchedulerInstance(json)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, schedulerInstance)
}

// @Summary Destroy SchedulerInstance
// @Description Destroy by id
// @Tags SchedulerInstance
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /scheduler-instances/{id} [delete]
func (h *Handlers) DestroySchedulerInstance(ctx *gin.Context) {
	var params types.SchedulerInstanceParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	err := h.service.DestroySchedulerInstance(params.ID)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.Status(http.StatusOK)
}

// @Summary Update SchedulerInstance
// @Description Update by json config
// @Tags SchedulerInstance
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Param SchedulerInstance body types.UpdateSchedulerInstanceRequest true "SchedulerInstance"
// @Success 200 {object} model.SchedulerInstance
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /scheduler-instances/{id} [patch]
func (h *Handlers) UpdateSchedulerInstance(ctx *gin.Context) {
	var params types.SchedulerInstanceParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.Error(err)
		return
	}

	var json types.UpdateSchedulerInstanceRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.Error(err)
		return
	}

	if json.SecurityGroupDomain != "" {
		schedulerInstance, err := h.service.UpdateSchedulerInstanceWithSecurityGroupDomain(params.ID, json)
		if err != nil {
			ctx.Error(err)
			return
		}

		ctx.JSON(http.StatusOK, schedulerInstance)
		return
	}

	schedulerInstance, err := h.service.UpdateSchedulerInstance(params.ID, json)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, schedulerInstance)
}

// @Summary Get SchedulerInstance
// @Description Get SchedulerInstance by id
// @Tags SchedulerInstance
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200 {object} model.SchedulerInstance
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /scheduler-instances/{id} [get]
func (h *Handlers) GetSchedulerInstance(ctx *gin.Context) {
	var params types.SchedulerInstanceParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	schedulerInstance, err := h.service.GetSchedulerInstance(params.ID)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, schedulerInstance)
}

// @Summary Get SchedulerInstances
// @Description Get SchedulerInstances
// @Tags SchedulerInstance
// @Accept json
// @Produce json
// @Param page query int true "current page" default(0)
// @Param per_page query int true "return max item count, default 10, max 50" default(10) minimum(2) maximum(50)
// @Success 200 {object} []model.SchedulerInstance
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /scheduler-instances [get]
func (h *Handlers) GetSchedulerInstances(ctx *gin.Context) {
	var query types.GetSchedulerInstancesQuery
	if err := ctx.ShouldBindQuery(&query); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	h.setPaginationDefault(&query.Page, &query.PerPage)
	schedulerInstances, err := h.service.GetSchedulerInstances(query)
	if err != nil {
		ctx.Error(err)
		return
	}

	totalCount, err := h.service.SchedulerInstanceTotalCount(query)
	if err != nil {
		ctx.Error(err)
		return
	}

	h.setPaginationLinkHeader(ctx, query.Page, query.PerPage, int(totalCount))
	ctx.JSON(http.StatusOK, schedulerInstances)
}
