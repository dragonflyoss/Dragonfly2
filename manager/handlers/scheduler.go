package handlers

import (
	"net/http"

	"d7y.io/dragonfly/v2/manager/types"
	"github.com/gin-gonic/gin"
)

// @Summary Create Scheduler
// @Description create by json config
// @Tags Scheduler
// @Accept json
// @Produce json
// @Param Scheduler body types.CreateSchedulerRequest true "Scheduler"
// @Success 200 {object} model.Scheduler
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /schedulers [post]
func (h *Handlers) CreateScheduler(ctx *gin.Context) {
	var json types.CreateSchedulerRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	scheduler, err := h.service.CreateScheduler(json)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, scheduler)
}

// @Summary Destroy Scheduler
// @Description Destroy by id
// @Tags Scheduler
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /schedulers/{id} [delete]
func (h *Handlers) DestroyScheduler(ctx *gin.Context) {
	var params types.SchedulerParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	err := h.service.DestroyScheduler(params.ID)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.Status(http.StatusOK)
}

// @Summary Update Scheduler
// @Description Update by json config
// @Tags Scheduler
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Param Scheduler body types.UpdateSchedulerRequest true "Scheduler"
// @Success 200 {object} model.Scheduler
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /schedulers/{id} [patch]
func (handler *Handlers) UpdateScheduler(ctx *gin.Context) {
	var params types.SchedulerParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.Error(err)
		return
	}

	var json types.UpdateSchedulerRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.Error(err)
		return
	}

	scheduler, err := handler.service.UpdateScheduler(params.ID, json)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, scheduler)
}

// @Summary Get Scheduler
// @Description Get Scheduler by id
// @Tags Scheduler
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200 {object} model.Scheduler
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /schedulers/{id} [get]
func (h *Handlers) GetScheduler(ctx *gin.Context) {
	var params types.SchedulerParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	scheduler, err := h.service.GetScheduler(params.ID)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, scheduler)
}

// @Summary Get Schedulers
// @Description Get Schedulers
// @Tags Scheduler
// @Accept json
// @Produce json
// @Param page query int true "current page" default(0)
// @Param per_page query int true "return max item count, default 10, max 50" default(10) minimum(2) maximum(50)
// @Success 200 {object} []model.Scheduler
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /schedulers [get]
func (h *Handlers) GetSchedulers(ctx *gin.Context) {
	var query types.GetSchedulersQuery
	if err := ctx.ShouldBindQuery(&query); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	h.setPaginationDefault(&query.Page, &query.PerPage)
	schedulers, err := h.service.GetSchedulers(query)
	if err != nil {
		ctx.Error(err)
		return
	}

	totalCount, err := h.service.SchedulerTotalCount()
	if err != nil {
		ctx.Error(err)
		return
	}

	h.setPaginationLinkHeader(ctx, query.Page, query.PerPage, int(totalCount))
	ctx.JSON(http.StatusOK, schedulers)
}
