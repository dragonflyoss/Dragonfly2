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

// @Summary Create Peer
// @Description Create by json config
// @Tags Peer
// @Accept json
// @Produce json
// @Param Peer body types.CreatePeerRequest true "Peer"
// @Success 200 {object} models.Peer
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /peers [post]
func (h *Handlers) CreatePeer(ctx *gin.Context) {
	var json types.CreatePeerRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	peer, err := h.service.CreatePeer(ctx.Request.Context(), json)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, peer)
}

// @Summary Destroy Peer
// @Description Destroy by id
// @Tags Peer
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /peers/{id} [delete]
func (h *Handlers) DestroyPeer(ctx *gin.Context) {
	var params types.PeerParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	if err := h.service.DestroyPeer(ctx.Request.Context(), params.ID); err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.Status(http.StatusOK)
}

// @Summary Get Peer
// @Description Get Peer by id
// @Tags Peer
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200 {object} models.Peer
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /peers/{id} [get]
func (h *Handlers) GetPeer(ctx *gin.Context) {
	var params types.PeerParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	peer, err := h.service.GetPeer(ctx.Request.Context(), params.ID)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, peer)
}

// @Summary Get Peers
// @Description Get Peers
// @Tags Peer
// @Accept json
// @Produce json
// @Param page query int true "current page" default(0)
// @Param per_page query int true "return max item count, default 10, max 50" default(10) minimum(2) maximum(50)
// @Success 200 {object} []models.Peer
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /peers [get]
func (h *Handlers) GetPeers(ctx *gin.Context) {
	var query types.GetPeersQuery
	if err := ctx.ShouldBindQuery(&query); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	h.setPaginationDefault(&query.Page, &query.PerPage)
	peers, count, err := h.service.GetPeers(ctx.Request.Context(), query)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	h.setPaginationLinkHeader(ctx, query.Page, query.PerPage, int(count))
	ctx.JSON(http.StatusOK, peers)
}
