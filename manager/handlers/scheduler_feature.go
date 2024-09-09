/*
 *     Copyright 2024 The Dragonfly Authors
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

// @Summary Get Scheudler Features
// @Description Get Scheduler Features
// @Tags Scheduler Feature
// @Accept json
// @Produce json
// @Success 200 {array} string
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /scheduler-features [get]
func (h *Handlers) GetSchedulerFeatures(ctx *gin.Context) {
	features := h.service.GetSchedulerFeatures(ctx.Request.Context())
	ctx.JSON(http.StatusOK, features)
}
