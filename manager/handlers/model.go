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
	"d7y.io/dragonfly/v2/manager/types"
	"github.com/gin-gonic/gin"
	"net/http"
)

func (h *Handlers) GetModelById(ctx *gin.Context) {
	var params types.ModelParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}
	var json types.ModelInfos
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}
	models, err := h.service.GetModelById(ctx.Request.Context(), params, json)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, models)
}

func (h *Handlers) GetModels(ctx *gin.Context) {
	var params types.ModelParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}
	var json types.ModelInfos
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}
	models, err := h.service.GetModels(ctx.Request.Context(), params, json)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, models)
}

func (h *Handlers) UpdateModelById(ctx *gin.Context) {
	var params types.ModelParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}
	var json types.ModelInfos
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}
	err := h.service.UpdateModelById(ctx.Request.Context(), params, json)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.Status(http.StatusOK)
}

func (h *Handlers) DeleteModelById(ctx *gin.Context) {
	var params types.ModelParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}
	var json types.ModelInfos
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}
	err := h.service.DeleteModelById(ctx.Request.Context(), params, json)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.Status(http.StatusOK)
}
