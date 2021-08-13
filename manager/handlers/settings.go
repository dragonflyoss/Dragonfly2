package handlers

import (
	"net/http"

	"d7y.io/dragonfly/v2/manager/types"
	"github.com/gin-gonic/gin"
)

// @Summary Create Setting
// @Description create by json config
// @Tags Setting
// @Accept json
// @Produce json
// @Param Setting body types.CreateSettingRequest true "Setting"
// @Success 200 {object} model.Setting
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /settings [post]
func (h *Handlers) CreateSetting(ctx *gin.Context) {
	var json types.CreateSettingRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	setting, err := h.Service.CreateSetting(json)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, setting)
}

// @Summary Destroy Setting
// @Description Destroy by key
// @Tags Setting
// @Accept json
// @Produce json
// @Param id path string true "key"
// @Success 200
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /settings/{id} [delete]
func (h *Handlers) DestroySetting(ctx *gin.Context) {
	var params types.SettingParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	err := h.Service.DestroySetting(params.Key)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.Status(http.StatusOK)
}

// @Summary Update Setting
// @Description Update by json config
// @Tags Setting
// @Accept json
// @Produce json
// @Param key path string true "key"
// @Param Setting body types.UpdateSettingRequest true "Setting"
// @Success 200 {object} model.Setting
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /settings/{id} [patch]
func (h *Handlers) UpdateSetting(ctx *gin.Context) {

	var params types.SettingParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.Error(err)
		return
	}
	var json types.UpdateSettingRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.Error(err)
		return
	}

	setting, err := h.Service.UpdateSetting(params.Key, json)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, setting)
}

// @Summary Get Settings
// @Description Get Settings
// @Tags Setting
// @Accept json
// @Produce json
// @Param page query int true "current page" default(0)
// @Param per_page query int true "return max item count, default 10, max 50" default(10) minimum(2) maximum(50)
// @Success 200 {object} []model.Setting
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /settings [get]
func (h *Handlers) GetSettings(ctx *gin.Context) {
	var query types.GetSettingsQuery
	if err := ctx.ShouldBindQuery(&query); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	h.setPaginationDefault(&query.Page, &query.PerPage)
	settings, err := h.Service.GetSettings(query)
	if err != nil {
		ctx.Error(err)
		return
	}

	totalCount, err := h.Service.SettingTotalCount()
	if err != nil {
		ctx.Error(err)
		return
	}

	h.setPaginationLinkHeader(ctx, query.Page, query.PerPage, int(totalCount))
	ctx.JSON(http.StatusOK, settings)
}
