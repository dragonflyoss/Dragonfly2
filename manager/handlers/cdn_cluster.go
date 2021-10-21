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

	"d7y.io/dragonfly/v2/manager/types"
	"github.com/gin-gonic/gin"
)

// @Summary Create CDNCluster
// @Description create by json config
// @Tags CDNCluster
// @Accept json
// @Produce json
// @Param CDNCluster body types.CreateCDNClusterRequest true "DNCluster"
// @Success 200 {object} model.CDNCluster
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /cdn-clusters [post]
func (h *Handlers) CreateCDNCluster(ctx *gin.Context) {
	var json types.CreateCDNClusterRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	if json.SecurityGroupDomain != "" {
		cdn, err := h.service.CreateCDNClusterWithSecurityGroupDomain(ctx.Request.Context(), json)
		if err != nil {
			ctx.Error(err)
			return
		}

		ctx.JSON(http.StatusOK, cdn)
		return
	}

	cdnCluster, err := h.service.CreateCDNCluster(ctx.Request.Context(), json)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, cdnCluster)
}

// @Summary Destroy CDNCluster
// @Description Destroy by id
// @Tags CDNCluster
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /cdn-clusters/{id} [delete]
func (h *Handlers) DestroyCDNCluster(ctx *gin.Context) {
	var params types.CDNClusterParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	if err := h.service.DestroyCDNCluster(ctx.Request.Context(), params.ID); err != nil {
		ctx.Error(err)
		return
	}

	ctx.Status(http.StatusOK)
}

// @Summary Update CDNCluster
// @Description Update by json config
// @Tags CDNCluster
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Param CDNCluster body types.UpdateCDNClusterRequest true "CDNCluster"
// @Success 200 {object} model.CDNCluster
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /cdn-clusters/{id} [patch]
func (h *Handlers) UpdateCDNCluster(ctx *gin.Context) {
	var params types.CDNClusterParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.Error(err)
		return
	}

	var json types.UpdateCDNClusterRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.Error(err)
		return
	}

	if json.SecurityGroupDomain != "" {
		cdn, err := h.service.UpdateCDNClusterWithSecurityGroupDomain(ctx.Request.Context(), params.ID, json)
		if err != nil {
			ctx.Error(err)
			return
		}

		ctx.JSON(http.StatusOK, cdn)
		return
	}

	cdnCluster, err := h.service.UpdateCDNCluster(ctx.Request.Context(), params.ID, json)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, cdnCluster)
}

// @Summary Get CDNCluster
// @Description Get CDNCluster by id
// @Tags CDNCluster
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200 {object} model.CDNCluster
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /cdn-clusters/{id} [get]
func (h *Handlers) GetCDNCluster(ctx *gin.Context) {
	var params types.CDNClusterParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	cdnCluster, err := h.service.GetCDNCluster(ctx.Request.Context(), params.ID)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, cdnCluster)
}

// @Summary Get CDNClusters
// @Description Get CDNClusters
// @Tags CDNCluster
// @Accept json
// @Produce json
// @Param page query int true "current page" default(0)
// @Param per_page query int true "return max item count, default 10, max 50" default(10) minimum(2) maximum(50)
// @Success 200 {object} []model.CDNCluster
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /cdn-clusters [get]
func (h *Handlers) GetCDNClusters(ctx *gin.Context) {
	var query types.GetCDNClustersQuery
	if err := ctx.ShouldBindQuery(&query); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	h.setPaginationDefault(&query.Page, &query.PerPage)
	cdns, err := h.service.GetCDNClusters(ctx.Request.Context(), query)
	if err != nil {
		ctx.Error(err)
		return
	}

	totalCount, err := h.service.CDNClusterTotalCount(ctx.Request.Context(), query)
	if err != nil {
		ctx.Error(err)
		return
	}

	h.setPaginationLinkHeader(ctx, query.Page, query.PerPage, int(totalCount))
	ctx.JSON(http.StatusOK, cdns)
}

// @Summary Add Instance to CDNCluster
// @Description Add CDN to CDNCluster
// @Tags CDNCluster
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Param cdn_id path string true "cdn id"
// @Success 200
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /cdn-clusters/{id}/cdns/{cdn_id} [put]
func (h *Handlers) AddCDNToCDNCluster(ctx *gin.Context) {
	var params types.AddCDNToCDNClusterParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	if err := h.service.AddCDNToCDNCluster(ctx.Request.Context(), params.ID, params.CDNID); err != nil {
		ctx.Error(err)
		return
	}

	ctx.Status(http.StatusOK)
}

// @Summary Add SchedulerCluster to CDNCluster
// @Description Add SchedulerCluster to CDNCluster
// @Tags CDNCluster
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Param scheduler_cluster_id path string true "scheduler cluster id"
// @Success 200
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /cdn-clusters/{id}/scheduler-clusters/{scheduler_cluster_id} [put]
func (h *Handlers) AddSchedulerClusterToCDNCluster(ctx *gin.Context) {
	var params types.AddSchedulerClusterToCDNClusterParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	if err := h.service.AddSchedulerClusterToCDNCluster(ctx.Request.Context(), params.ID, params.SchedulerClusterID); err != nil {
		ctx.Error(err)
		return
	}

	ctx.Status(http.StatusOK)
}
