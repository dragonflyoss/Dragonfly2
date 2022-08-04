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

	_ "d7y.io/dragonfly/v2/manager/model" // nolint
	"d7y.io/dragonfly/v2/manager/types"

	"github.com/gin-gonic/gin"
)

// @Summary Destroy Model
// @Description Destroy by id
// @Tags Model
// @Accept json
// @Produce json
// @Param scheduler_id path string true "scheduler_id"
// @Param id path string true "id"
// @Success 200
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /schedulers/{scheduler_id}/models/{id} [delete]
func (h *Handlers) DestoryModel(ctx *gin.Context) {
	var params types.ModelParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	if err := h.service.DestroyModel(ctx.Request.Context(), params); err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.Status(http.StatusOK)
}

// @Summary Update Model
// @Description Update by json config
// @Tags Model
// @Accept json
// @Produce json
// @Param scheduler_id path string true "scheduler_id"
// @Param id path string true "id"
// @Param Model body types.UpdateModelRequest true "Model"
// @Success 200 {object} model.Model
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /schedulers/{scheduler_id}/models/{id} [patch]
func (h *Handlers) UpdateModel(ctx *gin.Context) {
	var params types.ModelParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	var json types.UpdateModelRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	model, err := h.service.UpdateModel(ctx.Request.Context(), params, json)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, model)
}

// @Summary Get Model
// @Description Get Model by id
// @Tags Model
// @Accept json
// @Produce json
// @Param scheduler_id path string true "scheduler_id"
// @Param id path string true "id"
// @Success 200 {object} model.Model
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /schedulers/{scheduler_id}/models/{id} [get]
func (h *Handlers) GetModel(ctx *gin.Context) {
	var params types.ModelParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	model, err := h.service.GetModel(ctx.Request.Context(), params)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, model)
}

// @Summary Get Models
// @Description Get Models
// @Tags Model
// @Accept json
// @Produce json
// @Param scheduler_id path string true "scheduler_id"
// @Success 200 {object} []model.Model
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /schedulers/{scheduler_id}/models [get]
func (h *Handlers) GetModels(ctx *gin.Context) {
	var params types.GetModelsParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	models, err := h.service.GetModels(ctx.Request.Context(), params)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, models)
}

// @Summary Destroy Model Version
// @Description Destroy by id
// @Tags Model
// @Accept json
// @Produce json
// @Param scheduler_id path string true "scheduler_id"
// @Param model_id path string true "model_id"
// @Param id path string true "id"
// @Success 200
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /schedulers/{scheduler_id}/models/{model_id}/versions/{id} [delete]
func (h *Handlers) DestoryModelVersion(ctx *gin.Context) {
	var params types.ModelVersionParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	if err := h.service.DestroyModelVersion(ctx.Request.Context(), params); err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.Status(http.StatusOK)
}

// @Summary Get Model Version
// @Description Get Model Version by id
// @Tags Model
// @Accept json
// @Produce json
// @Param scheduler_id path string true "scheduler_id"
// @Param model_id path string true "model_id"
// @Param id path string true "id"
// @Success 200 {object} model.Model
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /schedulers/{scheduler_id}/models/{model_id}/versions/{id} [get]
func (h *Handlers) GetModelVersion(ctx *gin.Context) {
	var params types.ModelVersionParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	model, err := h.service.GetModelVersion(ctx.Request.Context(), params)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, model)
}

// @Summary Get Model Versions
// @Description Get Model Versions by id
// @Tags Model
// @Accept json
// @Produce json
// @Param scheduler_id path string true "scheduler_id"
// @Param model_id path string true "model_id"
// @Success 200 {object} []model.Model
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /schedulers/{scheduler_id}/models/{model_id}/versions [get]
func (h *Handlers) GetModelVersions(ctx *gin.Context) {
	var params types.GetModelVersionsParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	modelVersions, err := h.service.GetModelVersions(ctx.Request.Context(), params)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, modelVersions)
}
