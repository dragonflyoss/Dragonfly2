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
	_ "d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/manager/types"
)

// @Summary Create SecurityRule
// @Description create by json config
// @Tags SecurityRule
// @Accept json
// @Produce json
// @Param SecurityRule body types.CreateSecurityRuleRequest true "SecurityRule"
// @Success 200 {object} model.SecurityRule
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /security-rules [post]
func (h *Handlers) CreateSecurityRule(ctx *gin.Context) {
	var json types.CreateSecurityRuleRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	securityRule, err := h.service.CreateSecurityRule(ctx.Request.Context(), json)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, securityRule)
}

// @Summary Destroy SecurityRule
// @Description Destroy by id
// @Tags SecurityRule
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /securityRules/{id} [delete]
func (h *Handlers) DestroySecurityRule(ctx *gin.Context) {
	var params types.SecurityRuleParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	if err := h.service.DestroySecurityRule(ctx.Request.Context(), params.ID); err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.Status(http.StatusOK)
}

// @Summary Update SecurityRule
// @Description Update by json config
// @Tags SecurityRule
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Param SecurityRule body types.UpdateSecurityRuleRequest true "SecurityRule"
// @Success 200 {object} model.SecurityRule
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /security-rules/{id} [patch]
func (h *Handlers) UpdateSecurityRule(ctx *gin.Context) {
	var params types.SecurityRuleParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	var json types.UpdateSecurityRuleRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	securityRule, err := h.service.UpdateSecurityRule(ctx.Request.Context(), params.ID, json)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, securityRule)
}

// @Summary Get SecurityRule
// @Description Get SecurityRule by id
// @Tags SecurityRule
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200 {object} model.SecurityRule
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /security-rules/{id} [get]
func (h *Handlers) GetSecurityRule(ctx *gin.Context) {
	var params types.SecurityRuleParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	securityRule, err := h.service.GetSecurityRule(ctx.Request.Context(), params.ID)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, securityRule)
}

// @Summary Get SecurityRules
// @Description Get SecurityRules
// @Tags SecurityRule
// @Accept json
// @Produce json
// @Param page query int true "current page" default(0)
// @Param per_page query int true "return max item count, default 10, max 50" default(10) minimum(2) maximum(50)
// @Success 200 {object} []model.SecurityRule
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /security-rules [get]
func (h *Handlers) GetSecurityRules(ctx *gin.Context) {
	var query types.GetSecurityRulesQuery
	if err := ctx.ShouldBindQuery(&query); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	h.setPaginationDefault(&query.Page, &query.PerPage)
	securityRules, count, err := h.service.GetSecurityRules(ctx.Request.Context(), query)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	h.setPaginationLinkHeader(ctx, query.Page, query.PerPage, int(count))
	ctx.JSON(http.StatusOK, securityRules)
}
