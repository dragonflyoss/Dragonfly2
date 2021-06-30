package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// @Summary Get Health
// @Description Get app health
// @Tags Health
// @Accept json
// @Produce json
// @Success 200
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /healthy/*action [get]
func (h *Handlers) GetHealth(ctx *gin.Context) {
	ctx.JSON(http.StatusOK, "OK")
}
