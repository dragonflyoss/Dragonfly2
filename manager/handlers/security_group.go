/*
 *     Copyright 2020 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"

	// nolint
	_ "d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/manager/types"
)

// @Summary Create SecurityGroup
// @Description create by json config
// @Tags SecurityGroup
// @Accept json
// @Produce json
// @Param SecurityGroup body types.CreateSecurityGroupRequest true "SecurityGroup"
// @Success 200 {object} model.SecurityGroup
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /security-groups [post]
func (h *Handlers) CreateSecurityGroup(ctx *gin.Context) {
	var json types.CreateSecurityGroupRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	securityGroup, err := h.service.CreateSecurityGroup(ctx.Request.Context(), json)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
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
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /securityGroups/{id} [delete]
func (h *Handlers) DestroySecurityGroup(ctx *gin.Context) {
	var params types.SecurityGroupParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	if err := h.service.DestroySecurityGroup(ctx.Request.Context(), params.ID); err != nil {
		ctx.Error(err) // nolint: errcheck
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
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /security-groups/{id} [patch]
func (h *Handlers) UpdateSecurityGroup(ctx *gin.Context) {
	var params types.SecurityGroupParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	var json types.UpdateSecurityGroupRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	securityGroup, err := h.service.UpdateSecurityGroup(ctx.Request.Context(), params.ID, json)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
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
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /security-groups/{id} [get]
func (h *Handlers) GetSecurityGroup(ctx *gin.Context) {
	var params types.SecurityGroupParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	securityGroup, err := h.service.GetSecurityGroup(ctx.Request.Context(), params.ID)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
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
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /security-groups [get]
func (h *Handlers) GetSecurityGroups(ctx *gin.Context) {
	var query types.GetSecurityGroupsQuery
	if err := ctx.ShouldBindQuery(&query); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	h.setPaginationDefault(&query.Page, &query.PerPage)
	securityGroups, count, err := h.service.GetSecurityGroups(ctx.Request.Context(), query)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	h.setPaginationLinkHeader(ctx, query.Page, query.PerPage, int(count))
	ctx.JSON(http.StatusOK, securityGroups)
}

// @Summary Add Scheduler to SecurityGroup
// @Description Add Scheduler to SecurityGroup
// @Tags SecurityGroup
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Param scheduler_cluster_id path string true "scheduler cluster id"
// @Success 200
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /security-groups/{id}/scheduler-clusters/{scheduler_cluster_id} [put]
func (h *Handlers) AddSchedulerClusterToSecurityGroup(ctx *gin.Context) {
	var params types.AddSchedulerClusterToSecurityGroupParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	err := h.service.AddSchedulerClusterToSecurityGroup(ctx.Request.Context(), params.ID, params.SchedulerClusterID)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
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
// @Param cdn_cluster_id path string true "cdn cluster id"
// @Success 200
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /security-groups/{id}/cdn-clusters/{cdn_cluster_id} [put]
func (h *Handlers) AddCDNClusterToSecurityGroup(ctx *gin.Context) {
	var params types.AddCDNClusterToSecurityGroupParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	err := h.service.AddCDNClusterToSecurityGroup(ctx.Request.Context(), params.ID, params.CDNClusterID)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.Status(http.StatusOK)
}

// @Summary Add SecurityRule to SecurityGroup
// @Description Add SecurityRule to SecurityGroup
// @Tags SecurityGroup
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Param security_rule_id path string true "security rule id"
// @Success 200
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /security-groups/{id}/security-rules/{security_rule_id} [put]
func (h *Handlers) AddSecurityRuleToSecurityGroup(ctx *gin.Context) {
	var params types.AddSecurityRuleToSecurityGroupParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	err := h.service.AddSecurityRuleToSecurityGroup(ctx.Request.Context(), params.ID, params.SecurityRuleID)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.Status(http.StatusOK)
}

// @Summary Destroy SecurityRule to SecurityGroup
// @Description Destroy SecurityRule to SecurityGroup
// @Tags SecurityGroup
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Param security_rule_id path string true "security rule id"
// @Success 200
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /security-groups/{id}/security-rules/{security_rule_id} [delete]
func (h *Handlers) DestroySecurityRuleToSecurityGroup(ctx *gin.Context) {
	var params types.AddSecurityRuleToSecurityGroupParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	err := h.service.DestroySecurityRuleToSecurityGroup(ctx.Request.Context(), params.ID, params.SecurityRuleID)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.Status(http.StatusOK)
}
