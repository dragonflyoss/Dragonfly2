package handlers

import (
	"net/http"

	"d7y.io/dragonfly/v2/manager/types"
	"github.com/gin-gonic/gin"
)

// @Summary Create CDNInstance
// @Description create by json config
// @Tags CDNInstance
// @Accept json
// @Produce json
// @Param CDNInstance body types.CreateCDNInstanceRequest true "CDNInstance"
// @Success 200 {object} model.CDNInstance
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /cdn-instances [post]
func (h *Handlers) CreateCDNInstance(ctx *gin.Context) {
	var json types.CreateCDNInstanceRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	cdnInstance, err := h.service.CreateCDNInstance(json)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, cdnInstance)
}

// @Summary Destroy CDNInstance
// @Description Destroy by id
// @Tags CDNInstance
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /cdn-instances/{id} [delete]
func (h *Handlers) DestroyCDNInstance(ctx *gin.Context) {
	var params types.CDNInstanceParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	err := h.service.DestroyCDNInstance(params.ID)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.Status(http.StatusOK)
}

// @Summary Update CDNInstance
// @Description Update by json config
// @Tags CDNInstance
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Param CDNInstance body types.UpdateCDNInstanceRequest true "CDNInstance"
// @Success 200 {object} model.CDNInstance
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /cdn-instances/{id} [patch]
func (handler *Handlers) UpdateCDNInstance(ctx *gin.Context) {
	var params types.CDNInstanceParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.Error(err)
		return
	}

	var json types.UpdateCDNInstanceRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.Error(err)
		return
	}

	cdnInstance, err := handler.service.UpdateCDNInstance(params.ID, json)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, cdnInstance)
}

// @Summary Get CDNInstance
// @Description Get CDNInstance by id
// @Tags CDNInstance
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200 {object} model.CDNInstance
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /cdn-instances/{id} [get]
func (h *Handlers) GetCDNInstance(ctx *gin.Context) {
	var params types.CDNInstanceParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	cdnInstance, err := h.service.GetCDNInstance(params.ID)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, cdnInstance)
}

// @Summary Get CDNInstances
// @Description Get CDNInstances
// @Tags CDNInstance
// @Accept json
// @Produce json
// @Param page query int true "current page" default(0)
// @Param per_page query int true "return max item count, default 10, max 50" default(10) minimum(2) maximum(50)
// @Success 200 {object} []model.CDNInstance
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /cdn-instances [get]
func (h *Handlers) GetCDNInstances(ctx *gin.Context) {
	var query types.GetCDNInstancesQuery
	if err := ctx.ShouldBindQuery(&query); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	h.setPaginationDefault(&query.Page, &query.PerPage)
	cdnInstances, err := h.service.GetCDNInstances(query)
	if err != nil {
		ctx.Error(err)
		return
	}

	totalCount, err := h.service.CDNInstanceTotalCount()
	if err != nil {
		ctx.Error(err)
		return
	}

	h.setPaginationLinkHeader(ctx, query.Page, query.PerPage, int(totalCount))
	ctx.JSON(http.StatusOK, cdnInstances)
}
