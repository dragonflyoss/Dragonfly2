package handlers

import (
	"net/http"

	"d7y.io/dragonfly/v2/manager/types"
	"github.com/gin-gonic/gin"
)

// @Summary Create Preheat
// @Description create by json config
// @Tags Preheat
// @Accept json
// @Produce json
// @Param CDN body types.CreatePreheatRequest true "Preheat"
// @Success 200 {object} types.CreatePreheatResponse
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /preheats [post]
func (h *Handlers) CreatePreheat(ctx *gin.Context) {
	var json types.CreatePreheatRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	preheat, err := h.Service.CreatePreheat(json)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, preheat)
}

// @Summary Get Preheat
// @Description Get Preheat by id
// @Tags Preheat
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200 {object} types.GetPreheatResponse
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /preheats/{id} [get]
func (h *Handlers) GetPreheat(ctx *gin.Context) {
	var params types.PreheatParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	preheat, err := h.Service.GetPreheat(params.ID)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, preheat)
}
