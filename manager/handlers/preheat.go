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
	_ "d7y.io/dragonfly/v2/manager/models"
	"d7y.io/dragonfly/v2/manager/types"
)

// @Summary Create V1 Preheat
// @Description Create by json config
// @Tags Preheat
// @Accept json
// @Produce json
// @Param Preheat body types.CreateV1PreheatRequest true "Preheat"
// @Success 200 {object} types.CreateV1PreheatResponse
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /preheats [post]
func (h *Handlers) CreateV1Preheat(ctx *gin.Context) {
	var json types.CreateV1PreheatRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	preheat, err := h.service.CreateV1Preheat(ctx.Request.Context(), json)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, preheat)
}

// @Summary Get V1 Preheat
// @Description Get Preheat by id
// @Tags Preheat
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200 {object} types.GetV1PreheatResponse
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /preheats/{id} [get]
func (h *Handlers) GetV1Preheat(ctx *gin.Context) {
	var params types.V1PreheatParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	preheat, err := h.service.GetV1Preheat(ctx.Request.Context(), params.ID)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, preheat)
}
