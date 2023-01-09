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
	_ "d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/manager/types"
)

// @Summary Create Model
// @Description Create by id
// @Tags Model
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Param Model body types.CreateModelRequest true "Model"
// @Success 200 {object} types.Model
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /schedulers/{id}/models [post]
func (h *Handlers) CreateModel(ctx *gin.Context) {
	var params types.CreateModelParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	var json types.CreateModelRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	model, err := h.service.CreateModel(ctx.Request.Context(), params, json)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, model)
}

// @Summary Destroy Model
// @Description Destroy by id
// @Tags Model
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Param model_id path string true "model_id"
// @Success 200
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /schedulers/{id}/models/{model_id} [delete]
func (h *Handlers) DestroyModel(ctx *gin.Context) {
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
// @Param id path string true "id"
// @Param model_id path string true "model_id"
// @Param Model body types.UpdateModelRequest true "Model"
// @Success 200 {object} types.Model
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /schedulers/{id}/models/{model_id} [patch]
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
// @Param id path string true "id"
// @Param model_id path string true "model_id"
// @Success 200 {object} types.Model
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /schedulers/{id}/models/{model_id} [get]
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
// @Param id path string true "id"
// @Success 200 {object} []types.Model
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /schedulers/{id}/models [get]
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

// @Summary Create Model Version
// @Description Create by id
// @Tags Model
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Param model_id path string true "model_id"
// @Param ModelVersion body types.CreateModelVersionRequest true "ModelVersion"
// @Success 200 {object} types.ModelVersion
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /schedulers/{id}/models/{model_id}/versions [post]
func (h *Handlers) CreateModelVersion(ctx *gin.Context) {
	var params types.CreateModelVersionParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	var json types.CreateModelVersionRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	modelVersion, err := h.service.CreateModelVersion(ctx.Request.Context(), params, json)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, modelVersion)
}

// @Summary Destroy Model Version
// @Description Destroy by id
// @Tags Model
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Param model_id path string true "model_id"
// @Param version_id path string true "version_id"
// @Success 200
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /schedulers/{id}/models/{model_id}/versions/{version_id} [delete]
func (h *Handlers) DestroyModelVersion(ctx *gin.Context) {
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

// @Summary Update Model Version
// @Description Update by json config
// @Tags Model
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Param model_id path string true "model_id"
// @Param version_id path string true "version_id"
// @Param ModelVersion body types.UpdateModelVersionRequest true "ModelVersion"
// @Success 200 {object} types.ModelVersion
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /schedulers/{id}/models/{model_id}/versions/{version_id} [patch]
func (h *Handlers) UpdateModelVersion(ctx *gin.Context) {
	var params types.ModelVersionParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	var json types.UpdateModelVersionRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	modelVersion, err := h.service.UpdateModelVersion(ctx.Request.Context(), params, json)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, modelVersion)
}

// @Summary Get Model Version
// @Description Get Model Version by id
// @Tags Model
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Param model_id path string true "model_id"
// @Param version_id path string true "version_id"
// @Success 200 {object} types.ModelVersion
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /schedulers/{id}/models/{model_id}/versions/{version_id} [get]
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
// @Param id path string true "id"
// @Param model_id path string true "model_id"
// @Success 200 {object} []types.ModelVersion
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /schedulers/{id}/models/{model_id}/versions [get]
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
