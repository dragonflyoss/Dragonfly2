package handlers

import (
	"net/http"

	"d7y.io/dragonfly/v2/manager/types"
	"github.com/gin-gonic/gin"
)

// @Summary Get Endpoints
// @Description Get Endpoints by json config
// @Tags permission
// @Accept json
// @Produce json
// @Success 200 {object} RoutesInfo
// @Failure 400 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /endpoints [get]

func (h *Handlers) GetEndpoints(g *gin.Engine) func(ctx *gin.Context) {
	return func(ctx *gin.Context) {

		routesInfo := h.Service.GetEndpoints(g)

		ctx.JSON(http.StatusOK, routesInfo)
	}
}

// @Summary Create Permission
// @Description Create Permission by json config
// @Tags permission
// @Accept json
// @Produce json
// @Success 200
// @Failure 400 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /permission [post]

func (h *Handlers) CreatePermission(ctx *gin.Context) {
	var json types.PolicyRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}
	err := h.Service.CreatePermission(json)
	if err != nil {
		ctx.Error(err)
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"message": "create permission successfully"})
}

// @Summary Destory Permission
// @Description Destory Permission by json config
// @Tags permission
// @Accept json
// @Produce json
// @Success 200
// @Failure 400 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /permission [post]

func (h *Handlers) DestoryPermission(ctx *gin.Context) {
	var json types.PolicyRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}
	err := h.Service.DestoryPermission(json)
	if err != nil {
		ctx.Error(err)
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"message": "destroy permission successfully"})
}
