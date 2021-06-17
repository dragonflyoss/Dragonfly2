package handlers

import (
	"context"
	"net/http"

	"d7y.io/dragonfly/v2/manager/store"
	"d7y.io/dragonfly/v2/manager/types"
	"github.com/gin-gonic/gin"
)

// CreateSchedulerInstance godoc
// @Summary Add scheduler instance
// @Description add by json config
// @Tags SchedulerInstance
// @Accept  json
// @Produce  json
// @Param instance body types.SchedulerInstance true "Scheduler instance"
// @Success 200 {object} types.SchedulerInstance
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /scheduler/instances [post]
func (handler *Handlers) CreateSchedulerInstance(ctx *gin.Context) {
	var json types.CreateSchedulerInstanceRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.Error(err)
		return
	}

	schedulerInstance, err := handler.server.AddSchedulerInstance(context.TODO(), &json)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, schedulerInstance)
}

// DestroySchedulerInstance godoc
// @Summary Delete scheduler instance
// @Description Delete by instanceId
// @Tags SchedulerInstance
// @Accept  json
// @Produce  json
// @Param  id path string true "InstanceID"
// @Success 200 {string} string
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /scheduler/instances/{id} [delete]
func (handler *Handlers) DestroySchedulerInstance(ctx *gin.Context) {
	var params types.SchedulerInstanceParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.Error(err)
		return
	}

	schedulerInstance, err := handler.server.DeleteSchedulerInstance(context.TODO(), params.ID)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, schedulerInstance)
}

// UpdateSchedulerInstance godoc
// @Summary Update scheduler instance
// @Description Update by json scheduler instance
// @Tags SchedulerInstance
// @Accept  json
// @Produce  json
// @Param  id path string true "InstanceID"
// @Param  Instance body types.SchedulerInstance true "SchedulerInstance"
// @Success 200 {string} string
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /scheduler/instances/{id} [post]
func (handler *Handlers) UpdateSchedulerInstance(ctx *gin.Context) {
	var params types.SchedulerInstanceParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.Error(err)
		return
	}

	var json types.CreateSchedulerInstanceRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.Error(err)
		return
	}

	schedulerInstance, err := handler.server.UpdateSchedulerInstance(context.TODO(), params.ID, json)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, schedulerInstance)
}

// GetSchedulerInstance godoc
// @Summary Get scheduler instance
// @Description Get scheduler instance by InstanceID
// @Tags SchedulerInstance
// @Accept  json
// @Produce  json
// @Param id path string true "InstanceID"
// @Success 200 {object} types.SchedulerInstance
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /scheduler/instances/{id} [get]
func (handler *Handlers) GetSchedulerInstance(ctx *gin.Context) {
	var params types.SchedulerInstanceParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.Error(err)
		return
	}

	schedulerInstance, err := handler.server.GetSchedulerInstance(context.TODO(), params.ID)
	if err != nil {
		ctx.Error(err)
	}

	ctx.JSON(http.StatusOK, schedulerInstance)
}

// GetSchedulerInstances godoc
// @Summary List scheduler instances
// @Description List by object
// @Tags SchedulerInstance
// @Accept  json
// @Produce  json
// @Param marker query int true "begin marker of current page" default(0)
// @Param maxItemCount query int true "return max item count, default 10, max 50" default(10) minimum(10) maximum(50)
// @Success 200 {object} types.ListSchedulerInstancesResponse
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /scheduler/instances [get]
func (handler *Handlers) GetSchedulerInstances(ctx *gin.Context) {
	var query types.GetCDNInstancesQuery
	if err := ctx.ShouldBindQuery(&query); err != nil {
		ctx.Error(err)
		return
	}

	schedulerInstances, err := handler.server.ListSchedulerInstances(context.TODO(), store.WithMarker(query.Marker, query.MaxItemCount))
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, schedulerInstances)
}
