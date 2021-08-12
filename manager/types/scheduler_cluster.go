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

type SchedulerClusterParams struct {
	ID uint `uri:"id" binding:"required"`
}

type AddSchedulerToSchedulerClusterParams struct {
	ID          uint `uri:"id" binding:"required"`
	SchedulerID uint `uri:"scheduler_id" binding:"required"`
}

type CreateSchedulerClusterRequest struct {
	Name                string                 `json:"name" binding:"required"`
	BIO                 string                 `json:"bio" binding:"omitempty"`
	Config              map[string]interface{} `json:"config" binding:"required"`
	ClientConfig        map[string]interface{} `json:"client_config" binding:"required"`
	Scopes              map[string]interface{} `json:"scopes" binding:"omitempty"`
	IsDefault           bool                   `json:"is_default" binding:"omitempty"`
	SecurityGroupDomain string                 `json:"security_group_domain" binding:"omitempty"`
}

type UpdateSchedulerClusterRequest struct {
	Name                string                 `json:"name" binding:"omitempty"`
	BIO                 string                 `json:"bio" binding:"omitempty"`
	Config              map[string]interface{} `json:"config" binding:"omitempty"`
	ClientConfig        map[string]interface{} `json:"client_config" binding:"omitempty"`
	Scopes              map[string]interface{} `json:"scopes" binding:"omitempty"`
	IsDefault           bool                   `json:"is_default" binding:"omitempty"`
	SecurityGroupDomain string                 `json:"security_group_domain" binding:"omitempty"`
}

type GetSchedulerClustersQuery struct {
	Name    string `form:"name" binding:"omitempty"`
	Page    int    `form:"page" binding:"omitempty,gte=1"`
	PerPage int    `form:"per_page" binding:"omitempty,gte=1,lte=50"`
}
