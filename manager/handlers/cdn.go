package handlers

import (
	"net/http"

	"d7y.io/dragonfly/v2/manager/types"
	"github.com/gin-gonic/gin"
)

// @Summary Create CDN
// @Description create by json config
// @Tags CDN
// @Accept json
// @Produce json
// @Param CDN body types.CreateCDNRequest true "CDN"
// @Success 200 {object} model.CDN
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /cdns [post]
func (h *Handlers) CreateCDN(ctx *gin.Context) {
	var json types.CreateCDNRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	cdn, err := h.service.CreateCDN(json)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, cdn)
}

// @Summary Destroy CDN
// @Description Destroy by id
// @Tags CDN
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /cdns/{id} [delete]
func (h *Handlers) DestroyCDN(ctx *gin.Context) {
	var params types.CDNParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	err := h.service.DestroyCDN(params.ID)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.Status(http.StatusOK)
}

// @Summary Update CDN
// @Description Update by json config
// @Tags CDN
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Param CDN body types.UpdateCDNRequest true "CDN"
// @Success 200 {object} model.CDN
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /cdns/{id} [patch]
func (handler *Handlers) UpdateCDN(ctx *gin.Context) {
	var params types.CDNParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.Error(err)
		return
	}

	var json types.UpdateCDNRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.Error(err)
		return
	}

	cdn, err := handler.service.UpdateCDN(params.ID, json)
	if err != nil {
		ctx.Error(err)
	}

	ctx.JSON(http.StatusOK, cdn)
}

// @Summary Get CDN
// @Description Get CDN by id
// @Tags CDN
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200 {object} model.CDN
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /cdns/{id} [get]
func (h *Handlers) GetCDN(ctx *gin.Context) {
	var params types.CDNParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	cdn, err := h.service.GetCDN(params.ID)
	if err != nil {
		ctx.Error(err)
	}

	ctx.JSON(http.StatusOK, cdn)
}

// @Summary Get CDNs
// @Description Get CDNs
// @Tags CDN
// @Accept json
// @Produce json
// @Param page query int true "current page" default(0)
// @Param per_page query int true "return max item count, default 10, max 50" default(10) minimum(2) maximum(50)
// @Success 200 {object} types.GetCDNsQuery
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /cdns [get]
func (h *Handlers) GetCDNs(ctx *gin.Context) {
	var query types.GetCDNsQuery

	if err := ctx.ShouldBindQuery(&query); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	page, perPage := h.setPaginationDefault(query.Page, query.PerPage)
	cdns, err := h.service.GetCDNs(page, perPage)
	if err != nil {
		ctx.Error(err)
		return
	}

	totalCount, err := h.service.CDNTotalCount()
	if err != nil {
		ctx.Error(err)
		return
	}

	h.setPaginationLinkHeader(ctx, page, perPage, int(totalCount))
	ctx.JSON(http.StatusOK, cdns)
}
