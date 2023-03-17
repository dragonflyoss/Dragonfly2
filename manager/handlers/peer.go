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
)

// @Summary Get Peers
// @Description Get Peers
// @Tags Peer
// @Accept json
// @Produce json
// @Success 200 {object} []string
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /peers [get]
func (h *Handlers) GetPeers(ctx *gin.Context) {
	peers, err := h.service.GetPeers(ctx.Request.Context())
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, peers)
}
