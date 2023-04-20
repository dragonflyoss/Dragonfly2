/*
 *     Copyright 2023 The Dragonfly Authors
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
	_ "d7y.io/dragonfly/v2/manager/models"
	"d7y.io/dragonfly/v2/manager/types"
)

// @Summary Create Cluster
// @Description Create by json config
// @Tags Cluster
// @Accept json
// @Produce json
// @Param Cluster body types.CreateClusterRequest true "Cluster"
// @Success 200 {object} types.CreateClusterResponse
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /clusters [post]
func (h *Handlers) CreateCluster(ctx *gin.Context) {
	var json types.CreateClusterRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	cluster, err := h.service.CreateCluster(ctx.Request.Context(), json)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, cluster)
}

// @Summary Destroy Cluster
// @Description Destroy by id
// @Tags Cluster
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /clusters/{id} [delete]
func (h *Handlers) DestroyCluster(ctx *gin.Context) {
	var params types.ClusterParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	if err := h.service.DestroyCluster(ctx.Request.Context(), params.ID); err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.Status(http.StatusOK)
}

// @Summary Update Cluster
// @Description Update by json config
// @Tags Cluster
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Param Cluster body types.UpdateClusterRequest true "Cluster"
// @Success 200 {object} types.UpdateClusterResponse
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /clusters/{id} [patch]
func (h *Handlers) UpdateCluster(ctx *gin.Context) {
	var params types.ClusterParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	var json types.UpdateClusterRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	cluster, err := h.service.UpdateCluster(ctx.Request.Context(), params.ID, json)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, cluster)
}

// @Summary Get Cluster
// @Description Get Cluster by id
// @Tags Cluster
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200 {object} types.GetClusterResponse
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /clusters/{id} [get]
func (h *Handlers) GetCluster(ctx *gin.Context) {
	var params types.ClusterParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	cluster, err := h.service.GetCluster(ctx.Request.Context(), params.ID)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, cluster)
}

// @Summary Get Clusters
// @Description Get Clusters
// @Tags Cluster
// @Accept json
// @Produce json
// @Param page query int true "current page" default(0)
// @Param per_page query int true "return max item count, default 10, max 50" default(10) minimum(2) maximum(50)
// @Success 200 {object} []types.GetClusterResponse
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /clusters [get]
func (h *Handlers) GetClusters(ctx *gin.Context) {
	var query types.GetClustersQuery
	if err := ctx.ShouldBindQuery(&query); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	h.setPaginationDefault(&query.Page, &query.PerPage)
	clusters, count, err := h.service.GetClusters(ctx.Request.Context(), query)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	h.setPaginationLinkHeader(ctx, query.Page, query.PerPage, int(count))
	ctx.JSON(http.StatusOK, clusters)
}
