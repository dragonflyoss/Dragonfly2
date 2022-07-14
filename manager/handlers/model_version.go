package handlers

import (
	"d7y.io/dragonfly/v2/manager/types"
	"github.com/gin-gonic/gin"
	"net/http"
)

func (h *Handlers) GetVerison(ctx *gin.Context) {
	var params types.ModelParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}
	var json types.ModelInfos
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}
	models, err := h.service.GetVersion(ctx.Request.Context(), params, json)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, models)
}

func (h *Handlers) GetVersions(ctx *gin.Context) {
	var json types.ModelInfos
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}
	versions, err := h.service.GetVersions(ctx.Request.Context(), json)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, versions)
}

func (h *Handlers) UpdateVersion(ctx *gin.Context) {
	var params types.ModelParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}
	var json types.ModelInfos
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}
	err := h.service.UpdateVersion(ctx.Request.Context(), params, json)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.Status(http.StatusOK)
}

func (h *Handlers) DeleteVersion(ctx *gin.Context) {
	var params types.ModelParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}
	var json types.ModelInfos
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}
	err := h.service.DeleteVersion(ctx.Request.Context(), params, json)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.Status(http.StatusOK)
}
