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

type ApplicationParams struct {
	ID uint `uri:"id" binding:"required"`
}

type AddSchedulerClusterToApplicationParams struct {
	ID                 uint `uri:"id" binding:"required"`
	SchedulerClusterID uint `uri:"scheduler_cluster_id" binding:"required"`
}

type DeleteSchedulerClusterToApplicationParams struct {
	ID                 uint `uri:"id" binding:"required"`
	SchedulerClusterID uint `uri:"scheduler_cluster_id" binding:"required"`
}

type AddCDNClusterToApplicationParams struct {
	ID           uint `uri:"id" binding:"required"`
	CDNClusterID uint `uri:"cdn_cluster_id" binding:"required"`
}

type DeleteCDNClusterToApplicationParams struct {
	ID           uint `uri:"id" binding:"required"`
	CDNClusterID uint `uri:"cdn_cluster_id" binding:"required"`
}

type CreateApplicationRequest struct {
	Name              string `json:"name" binding:"required"`
	DownloadRateLimit string `json:"download_rate_limit" binding:"omitempty"`
	URL               string `json:"url" binding:"omitempty"`
	BIO               string `json:"bio" binding:"omitempty"`
	UserID            uint   `json:"user_id" binding:"omitempty"`
	State             string `json:"state" binding:"required,oneof=enable disable"`
}

type UpdateApplicationRequest struct {
	Name              string `json:"name" binding:"required"`
	DownloadRateLimit string `json:"download_rate_limit" binding:"required"`
	URL               string `json:"url" binding:"required"`
	State             string `json:"state" binding:"required,oneof=enable disable"`
	BIO               string `json:"bio" binding:"omitempty"`
	UserID            uint   `json:"user_id" binding:"omitempty"`
}

type GetApplicationsQuery struct {
	Page    int `form:"page" binding:"omitempty,gte=1"`
	PerPage int `form:"per_page" binding:"omitempty,gte=1,lte=50"`
}
