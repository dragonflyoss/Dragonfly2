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

type CreateJobRequest struct {
	BIO                 string         `json:"bio" binding:"omitempty"`
	Type                string         `json:"type" binding:"required"`
	Args                map[string]any `json:"args" binding:"omitempty"`
	Result              map[string]any `json:"result" binding:"omitempty"`
	UserID              uint           `json:"user_id" binding:"omitempty"`
	SeedPeerClusterIDs  []uint         `json:"seed_peer_cluster_ids" binding:"omitempty"`
	SchedulerClusterIDs []uint         `json:"scheduler_cluster_ids" binding:"omitempty"`
}

type UpdateJobRequest struct {
	BIO    string `json:"bio" binding:"omitempty"`
	UserID uint   `json:"user_id" binding:"omitempty"`
}

type JobParams struct {
	ID uint `uri:"id" binding:"required"`
}

type GetJobsQuery struct {
	Type    string `form:"type" binding:"omitempty"`
	State   string `form:"state" binding:"omitempty,oneof=PENDING RECEIVED STARTED RETRY SUCCESS FAILURE"`
	UserID  uint   `form:"user_id" binding:"omitempty"`
	Page    int    `form:"page" binding:"omitempty,gte=1"`
	PerPage int    `form:"per_page" binding:"omitempty,gte=1,lte=10000000"`
}

type CreatePreheatJobRequest struct {
	BIO                 string         `json:"bio" binding:"omitempty"`
	Type                string         `json:"type" binding:"required"`
	Args                PreheatArgs    `json:"args" binding:"omitempty"`
	Result              map[string]any `json:"result" binding:"omitempty"`
	UserID              uint           `json:"user_id" binding:"omitempty"`
	SchedulerClusterIDs []uint         `json:"scheduler_cluster_ids" binding:"omitempty"`
}

type PreheatArgs struct {
	// Type is the preheating type, support image and file.
	Type string `json:"type" binding:"required,oneof=image file"`

	// URL is the image url for preheating.
	URL string `json:"url" binding:"required"`

	// Tag is the tag for preheating.
	Tag string `json:"tag" binding:"omitempty"`

	// FilteredQueryParams is the filtered query params for preheating.
	FilteredQueryParams string `json:"filteredQueryParams" binding:"omitempty"`

	// Headers is the http headers for authentication.
	Headers map[string]string `json:"headers" binding:"omitempty"`

	// Username is the username for authentication.
	Username string `json:"username" binding:"omitempty"`

	// Password is the password for authentication.
	Password string `json:"password" binding:"omitempty"`

	// The image type preheating task can specify the image architecture type. eg: linux/amd64.
	Platform string `json:"platform" binding:"omitempty"`
}
