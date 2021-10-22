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
	BIO                 string                 `uri:"bio" binding:"omitempty"`
	Type                string                 `uri:"type" binding:"required"`
	Status              string                 `uri:"status" binding:"required,oneof=PENDING RECEIVED STARTED RETRY SUCCESS FAILURE"`
	Args                map[string]interface{} `uri:"args" binding:"omitempty"`
	Result              map[string]interface{} `uri:"result" binding:"omitempty"`
	UserID              uint                   `uri:"user_id" binding:"required"`
	CDNClusterIDs       []uint                 `uri:"cdn_cluster_ids" binding:"omitempty"`
	SchedulerClusterIDs []uint                 `uri:"scheduler_cluster_ids" binding:"omitempty"`
}

type UpdateJobRequest struct {
	BIO    string `uri:"bio" binding:"omitempty"`
	UserID uint   `uri:"user_id" binding:"required"`
}

type JobParams struct {
	ID uint `uri:"id" binding:"required"`
}

type GetJobsQuery struct {
	Type    string `form:"type" binding:"omitempty"`
	Status  string `form:"status" binding:"omitempty"`
	UserID  uint   `form:"user_id" binding:"omitempty"`
	Page    int    `form:"page" binding:"omitempty,gte=1"`
	PerPage int    `form:"per_page" binding:"omitempty,gte=1,lte=50"`
}

type CreatePreheatRequest struct {
	BIO                 string                 `uri:"bio" binding:"omitempty"`
	Type                string                 `uri:"type" binding:"required"`
	Status              string                 `uri:"status" binding:"required,oneof=PENDING RECEIVED STARTED RETRY SUCCESS FAILURE"`
	Args                PreheatArgs            `uri:"args" binding:"omitempty"`
	Result              map[string]interface{} `uri:"result" binding:"omitempty"`
	UserID              uint                   `uri:"user_id" binding:"required"`
	SchedulerClusterIDs []uint                 `uri:"scheduler_cluster_ids" binding:"required"`
}

type PreheatArgs struct {
	Type    string            `json:"type" binding:"required,oneof=image file"`
	URL     string            `json:"url" binding:"required"`
	Filter  string            `json:"filter" binding:"omitempty"`
	Headers map[string]string `json:"headers" binding:"omitempty"`
}
