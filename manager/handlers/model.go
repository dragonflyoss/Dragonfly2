package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"

	_ "d7y.io/dragonfly/v2/manager/models" // nolint
	"d7y.io/dragonfly/v2/manager/types"
)

// @Summary Destroy Model
// @Description Destroy by id
// @Tags Model
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /models/{id} [delete]
func (h *Handlers) DestroyModel(ctx *gin.Context) {
	var params types.ModelParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	if err := h.service.DestroyModel(ctx.Request.Context(), params.ID); err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.Status(http.StatusOK)
}

// @Summary Update Model
// @Description Update by json config
// @Tags Model
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Param Model body types.UpdateModelRequest true "Model"
// @Success 200 {object} models.Model
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /models/{id} [patch]
func (h *Handlers) UpdateModel(ctx *gin.Context) {
	var params types.ModelParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	var json types.UpdateModelRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	model, err := h.service.UpdateModel(ctx.Request.Context(), params.ID, json)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, model)
}

// @Summary Get Model
// @Description Get Model by id
// @Tags Model
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200 {object} models.Model
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /models/{id} [get]
func (h *Handlers) GetModel(ctx *gin.Context) {
	var params types.ModelParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	model, err := h.service.GetModel(ctx.Request.Context(), params.ID)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, model)
}

// @Summary Get Models
// @Description Get Models
// @Tags Model
// @Accept json
// @Produce json
// @Success 200 {object} []models.Model
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /models [get]
func (h *Handlers) GetModels(ctx *gin.Context) {
	var query types.GetModelsQuery
	if err := ctx.ShouldBindQuery(&query); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	h.setPaginationDefault(&query.Page, &query.PerPage)
	models, count, err := h.service.GetModels(ctx.Request.Context(), query)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	h.setPaginationLinkHeader(ctx, query.Page, query.PerPage, int(count))
	ctx.JSON(http.StatusOK, models)
}
