package handlers

import (
	"net/http"

	"d7y.io/dragonfly/v2/manager/types"
	"github.com/gin-gonic/gin"
)

// @Summary Create Preheat
// @Description create by json config
// @Tags CDN
// @Accept json
// @Produce json
// @Param CDN body types.CreatePreheatRequest true "Preheat"
// @Success 200
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

	if err := h.Service.CreatePreheat(json); err != nil {
		ctx.Error(err)
		return
	}

	ctx.Status(http.StatusOK)
}
