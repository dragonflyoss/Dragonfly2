/*
 *     Copyright 2022 The Dragonfly Authors
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

// @Summary Create SeedPeerCluster
// @Description Create by json config
// @Tags SeedPeerCluster
// @Accept json
// @Produce json
// @Param SeedPeerCluster body types.CreateSeedPeerClusterRequest true "DNCluster"
// @Success 200 {object} models.SeedPeerCluster
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /seed-peer-clusters [post]
func (h *Handlers) CreateSeedPeerCluster(ctx *gin.Context) {
	var json types.CreateSeedPeerClusterRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	seedPeerCluster, err := h.service.CreateSeedPeerCluster(ctx.Request.Context(), json)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, seedPeerCluster)
}

// @Summary Destroy SeedPeerCluster
// @Description Destroy by id
// @Tags SeedPeerCluster
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /seed-peer-clusters/{id} [delete]
func (h *Handlers) DestroySeedPeerCluster(ctx *gin.Context) {
	var params types.SeedPeerClusterParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	if err := h.service.DestroySeedPeerCluster(ctx.Request.Context(), params.ID); err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.Status(http.StatusOK)
}

// @Summary Update SeedPeerCluster
// @Description Update by json config
// @Tags SeedPeerCluster
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Param SeedPeerCluster body types.UpdateSeedPeerClusterRequest true "SeedPeerCluster"
// @Success 200 {object} models.SeedPeerCluster
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /seed-peer-clusters/{id} [patch]
func (h *Handlers) UpdateSeedPeerCluster(ctx *gin.Context) {
	var params types.SeedPeerClusterParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	var json types.UpdateSeedPeerClusterRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	seedPeerCluster, err := h.service.UpdateSeedPeerCluster(ctx.Request.Context(), params.ID, json)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, seedPeerCluster)
}

// @Summary Get SeedPeerCluster
// @Description Get SeedPeerCluster by id
// @Tags SeedPeerCluster
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200 {object} models.SeedPeerCluster
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /seed-peer-clusters/{id} [get]
func (h *Handlers) GetSeedPeerCluster(ctx *gin.Context) {
	var params types.SeedPeerClusterParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	seedPeerCluster, err := h.service.GetSeedPeerCluster(ctx.Request.Context(), params.ID)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, seedPeerCluster)
}

// @Summary Get SeedPeerClusters
// @Description Get SeedPeerClusters
// @Tags SeedPeerCluster
// @Accept json
// @Produce json
// @Param page query int true "current page" default(0)
// @Param per_page query int true "return max item count, default 10, max 50" default(10) minimum(2) maximum(50)
// @Success 200 {object} []models.SeedPeerCluster
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /seed-peer-clusters [get]
func (h *Handlers) GetSeedPeerClusters(ctx *gin.Context) {
	var query types.GetSeedPeerClustersQuery
	if err := ctx.ShouldBindQuery(&query); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	h.setPaginationDefault(&query.Page, &query.PerPage)
	seedPeers, count, err := h.service.GetSeedPeerClusters(ctx.Request.Context(), query)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	h.setPaginationLinkHeader(ctx, query.Page, query.PerPage, int(count))
	ctx.JSON(http.StatusOK, seedPeers)
}

// @Summary Add Instance to SeedPeerCluster
// @Description Add SeedPeer to SeedPeerCluster
// @Tags SeedPeerCluster
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Param seed_peer_id path string true "seed peer id"
// @Success 200
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /seed-peer-clusters/{id}/seed-peers/{seed_peer_id} [put]
func (h *Handlers) AddSeedPeerToSeedPeerCluster(ctx *gin.Context) {
	var params types.AddSeedPeerToSeedPeerClusterParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	if err := h.service.AddSeedPeerToSeedPeerCluster(ctx.Request.Context(), params.ID, params.SeedPeerID); err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.Status(http.StatusOK)
}

// @Summary Add SchedulerCluster to SeedPeerCluster
// @Description Add SchedulerCluster to SeedPeerCluster
// @Tags SeedPeerCluster
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Param scheduler_cluster_id path string true "scheduler cluster id"
// @Success 200
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /seed-peer-clusters/{id}/scheduler-clusters/{scheduler_cluster_id} [put]
func (h *Handlers) AddSchedulerClusterToSeedPeerCluster(ctx *gin.Context) {
	var params types.AddSchedulerClusterToSeedPeerClusterParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	if err := h.service.AddSchedulerClusterToSeedPeerCluster(ctx.Request.Context(), params.ID, params.SchedulerClusterID); err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.Status(http.StatusOK)
}
