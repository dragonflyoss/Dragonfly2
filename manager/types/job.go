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

import "time"

const (
	// SinglePeerScope represents the scope that only single peer will be preheated.
	SinglePeerScope = "single_peer"

	// AllPeersScope represents the scope that all peers will be preheated.
	AllPeersScope = "all_peers"
)

const (
	// DefaultPreheatConcurrentCount is the default concurrent count for preheating all peers.
	DefaultPreheatConcurrentCount = 50

	// DefaultJobTimeout is the default timeout for executing job.
	DefaultJobTimeout = 30 * time.Minute
)

type CreateJobRequest struct {
	BIO                 string         `json:"bio" binding:"omitempty"`
	Type                string         `json:"type" binding:"required"`
	Args                map[string]any `json:"args" binding:"omitempty"`
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
	BIO                 string      `json:"bio" binding:"omitempty"`
	Type                string      `json:"type" binding:"required"`
	Args                PreheatArgs `json:"args" binding:"omitempty"`
	UserID              uint        `json:"user_id" binding:"omitempty"`
	SchedulerClusterIDs []uint      `json:"scheduler_cluster_ids" binding:"omitempty"`
}

type PreheatArgs struct {
	// Type is the preheating type, support image and file.
	Type string `json:"type" binding:"required,oneof=image file"`

	// URL is the image url for preheating.
	URL string `json:"url" binding:"required"`

	// Tag is the tag for preheating.
	Tag string `json:"tag" binding:"omitempty"`

	// FilteredQueryParams is the filtered query params for preheating.
	FilteredQueryParams string `json:"filtered_query_params" binding:"omitempty"`

	// Headers is the http headers for authentication.
	Headers map[string]string `json:"headers" binding:"omitempty"`

	// Username is the username for authentication.
	Username string `json:"username" binding:"omitempty"`

	// Password is the password for authentication.
	Password string `json:"password" binding:"omitempty"`

	// The image type preheating task can specify the image architecture type. eg: linux/amd64.
	Platform string `json:"platform" binding:"omitempty"`

	// Scope is the scope for preheating, default is single_peer.
	Scope string `json:"scope" binding:"omitempty,oneof=single_peer all_peers"`

	// BatchSize is the batch size for preheating all peers, default is 50.
	ConcurrentCount int64 `json:"concurrent_count" binding:"omitempty,gte=1,lte=500"`

	// Timeout is the timeout for preheating, default is 30 minutes.
	Timeout time.Duration `json:"timeout" binding:"omitempty"`
}

type CreateGetTaskJobRequest struct {
	BIO                 string      `json:"bio" binding:"omitempty"`
	Type                string      `json:"type" binding:"required"`
	Args                GetTaskArgs `json:"args" binding:"omitempty"`
	UserID              uint        `json:"user_id" binding:"omitempty"`
	SchedulerClusterIDs []uint      `json:"scheduler_cluster_ids" binding:"omitempty"`
}

type GetTaskArgs struct {
	TaskID string `json:"task_id" binding:"required"`
}

type CreateDeleteTaskJobRequest struct {
	BIO                 string         `json:"bio" binding:"omitempty"`
	Type                string         `json:"type" binding:"required"`
	Args                DeleteTaskArgs `json:"args" binding:"omitempty"`
	UserID              uint           `json:"user_id" binding:"omitempty"`
	SchedulerClusterIDs []uint         `json:"scheduler_cluster_ids" binding:"omitempty"`
}

type DeleteTaskArgs struct {
	// TaskID is the task id for deleting.
	TaskID string `json:"task_id" binding:"required"`

	// Timeout is the timeout for deleting, default is 30 minutes.
	Timeout time.Duration `json:"timeout" binding:"omitempty"`
}
