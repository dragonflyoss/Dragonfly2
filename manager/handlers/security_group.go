package handlers

import (
	"net/http"

	"d7y.io/dragonfly/v2/manager/types"
	"github.com/gin-gonic/gin"
)

// @Summary Create SecurityGroup
// @Description create by json config
// @Tags SecurityGroup
// @Accept json
// @Produce json
// @Param SecurityGroup body types.CreateSecurityGroupRequest true "SecurityGroup"
// @Success 200 {object} model.SecurityGroup
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /security-groups [post]
func (h *Handlers) CreateSecurityGroup(ctx *gin.Context) {
	var json types.CreateSecurityGroupRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	securityGroup, err := h.service.CreateSecurityGroup(json)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, securityGroup)
}

// @Summary Destroy SecurityGroup
// @Description Destroy by id
// @Tags SecurityGroup
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /securityGroups/{id} [delete]
func (h *Handlers) DestroySecurityGroup(ctx *gin.Context) {
	var params types.SecurityGroupParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	err := h.service.DestroySecurityGroup(params.ID)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.Status(http.StatusOK)
}

// @Summary Update SecurityGroup
// @Description Update by json config
// @Tags SecurityGroup
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Param SecurityGroup body types.UpdateSecurityGroupRequest true "SecurityGroup"
// @Success 200 {object} model.SecurityGroup
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /security-groups/{id} [patch]
func (h *Handlers) UpdateSecurityGroup(ctx *gin.Context) {
	var params types.SecurityGroupParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.Error(err)
		return
	}

	var json types.UpdateSecurityGroupRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.Error(err)
		return
	}

	securityGroup, err := h.service.UpdateSecurityGroup(params.ID, json)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, securityGroup)
}

// @Summary Get SecurityGroup
// @Description Get SecurityGroup by id
// @Tags SecurityGroup
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200 {object} model.SecurityGroup
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /security-groups/{id} [get]
func (h *Handlers) GetSecurityGroup(ctx *gin.Context) {
	var params types.SecurityGroupParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	securityGroup, err := h.service.GetSecurityGroup(params.ID)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, securityGroup)
}

// @Summary Get SecurityGroups
// @Description Get SecurityGroups
// @Tags SecurityGroup
// @Accept json
// @Produce json
// @Param page query int true "current page" default(0)
// @Param per_page query int true "return max item count, default 10, max 50" default(10) minimum(2) maximum(50)
// @Success 200 {object} []model.SecurityGroup
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /security-groups[get]
func (h *Handlers) GetSecurityGroups(ctx *gin.Context) {
	var query types.GetSecurityGroupsQuery
	if err := ctx.ShouldBindQuery(&query); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	h.setPaginationDefault(&query.Page, &query.PerPage)
	securityGroups, err := h.service.GetSecurityGroups(query)
	if err != nil {
		ctx.Error(err)
		return
	}

	totalCount, err := h.service.SecurityGroupTotalCount(query)
	if err != nil {
		ctx.Error(err)
		return
	}

	h.setPaginationLinkHeader(ctx, query.Page, query.PerPage, int(totalCount))
	ctx.JSON(http.StatusOK, securityGroups)
}

// @Summary Add Scheduler to SecurityGroup
// @Description Add Scheduler to SecurityGroup
// @Tags SecurityGroup
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Param instance_id path string true "instance id"
// @Success 200
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /security-groups/{id}/scheduler-clusters/{scheduler_cluster_id} [put]
func (h *Handlers) AddSchedulerClusterToSecurityGroup(ctx *gin.Context) {
	var params types.AddSchedulerClusterToSecurityGroupParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	err := h.service.AddSchedulerClusterToSecurityGroup(params.ID, params.SchedulerClusterID)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.Status(http.StatusOK)
}

// @Summary Add CDN to SecurityGroup
// @Description Add CDN to SecurityGroup
// @Tags SecurityGroup
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Param instance_id path string true "instance id"
// @Success 200
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /security-groups/{id}/cdn-clusters/{cdn_cluster_id} [put]
func (h *Handlers) AddCDNClusterToSecurityGroup(ctx *gin.Context) {
	var params types.AddCDNClusterToSecurityGroupParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	err := h.service.AddCDNClusterToSecurityGroup(params.ID, params.CDNClusterID)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.Status(http.StatusOK)
}
