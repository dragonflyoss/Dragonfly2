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

package types

type CallSystemParams struct {
	ID uint `uri:"id" binding:"required"`
}

type AddSchedulerClusterToCallSystemParams struct {
	ID                 uint `uri:"id" binding:"required"`
	SchedulerClusterID uint `uri:"scheduler_cluster_id" binding:"required"`
}

type DeleteSchedulerClusterToCallSystemParams struct {
	ID                 uint `uri:"id" binding:"required"`
	SchedulerClusterID uint `uri:"scheduler_cluster_id" binding:"required"`
}

type CreateCallSystemRequest struct {
	Name           string   `json:"name" binding:"required"`
	DisplayName    string   `json:"display_name" binding:"required"`
	LimitRate      string   `json:"limit_rate" binding:"required"`
	URLRegexs      []string `json:"url_regexs" binding:"required"`
	TermOfValidity string   `json:"term_of_validity" binding:"required"`
	IsEnable       bool     `json:"is_enable" binding:"omitempty"`
}

type UpdateCallSystemRequest struct {
	Name           string   `json:"name" binding:"required"`
	DisplayName    string   `json:"display_name" binding:"required"`
	LimitRate      string   `json:"limit_rate" binding:"required"`
	URLRegexs      []string `json:"url_regexs" binding:"required"`
	TermOfValidity string   `json:"term_of_validity" binding:"required"`
	IsEnable       bool     `json:"is_enable" binding:"omitempty"`
}

type GetCallSystemsQuery struct {
	Name    string `form:"name" binding:"omitempty"`
	Page    int    `form:"page" binding:"omitempty,gte=1"`
	PerPage int    `form:"per_page" binding:"omitempty,gte=1,lte=50"`
}
