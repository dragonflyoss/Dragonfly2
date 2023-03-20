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

// @Summary Create SeedPeer
// @Description Create by json config
// @Tags SeedPeer
// @Accept json
// @Produce json
// @Param SeedPeer body types.CreateSeedPeerRequest true "SeedPeer"
// @Success 200 {object} models.SeedPeer
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /seed-peers [post]
func (h *Handlers) CreateSeedPeer(ctx *gin.Context) {
	var json types.CreateSeedPeerRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	seedPeer, err := h.service.CreateSeedPeer(ctx.Request.Context(), json)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, seedPeer)
}

// @Summary Destroy SeedPeer
// @Description Destroy by id
// @Tags SeedPeer
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /seed-peers/{id} [delete]
func (h *Handlers) DestroySeedPeer(ctx *gin.Context) {
	var params types.SeedPeerParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	if err := h.service.DestroySeedPeer(ctx.Request.Context(), params.ID); err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.Status(http.StatusOK)
}

// @Summary Update SeedPeer
// @Description Update by json config
// @Tags SeedPeer
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Param SeedPeer body types.UpdateSeedPeerRequest true "SeedPeer"
// @Success 200 {object} models.SeedPeer
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /seed-peers/{id} [patch]
func (h *Handlers) UpdateSeedPeer(ctx *gin.Context) {
	var params types.SeedPeerParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	var json types.UpdateSeedPeerRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	seedPeer, err := h.service.UpdateSeedPeer(ctx.Request.Context(), params.ID, json)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, seedPeer)
}

// @Summary Get SeedPeer
// @Description Get SeedPeer by id
// @Tags SeedPeer
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200 {object} models.SeedPeer
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /seed-peers/{id} [get]
func (h *Handlers) GetSeedPeer(ctx *gin.Context) {
	var params types.SeedPeerParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	seedPeer, err := h.service.GetSeedPeer(ctx.Request.Context(), params.ID)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, seedPeer)
}

// @Summary Get SeedPeers
// @Description Get SeedPeers
// @Tags SeedPeer
// @Accept json
// @Produce json
// @Param page query int true "current page" default(0)
// @Param per_page query int true "return max item count, default 10, max 50" default(10) minimum(2) maximum(50)
// @Success 200 {object} []models.SeedPeer
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /seed-peers [get]
func (h *Handlers) GetSeedPeers(ctx *gin.Context) {
	var query types.GetSeedPeersQuery
	if err := ctx.ShouldBindQuery(&query); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	h.setPaginationDefault(&query.Page, &query.PerPage)
	seedPeers, count, err := h.service.GetSeedPeers(ctx.Request.Context(), query)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	h.setPaginationLinkHeader(ctx, query.Page, query.PerPage, int(count))
	ctx.JSON(http.StatusOK, seedPeers)
}
