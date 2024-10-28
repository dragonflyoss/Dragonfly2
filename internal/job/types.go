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

package job

import (
	"time"
)

// PreheatRequest defines the request parameters for preheating.
type PreheatRequest struct {
	URL                 string            `json:"url" validate:"required,url"`
	Tag                 string            `json:"tag" validate:"omitempty"`
	FilteredQueryParams string            `json:"filtered_query_params" validate:"omitempty"`
	Headers             map[string]string `json:"headers" validate:"omitempty"`
	Application         string            `json:"application" validate:"omitempty"`
	Priority            int32             `json:"priority" validate:"omitempty"`
	Scope               string            `json:"scope" validate:"omitempty"`
	ConcurrentCount     int64             `json:"concurrent_count" validate:"omitempty"`
	CertificateChain    [][]byte          `json:"certificate_chain" validate:"omitempty"`
	InsecureSkipVerify  bool              `json:"insecure_skip_verify" validate:"omitempty"`
	Timeout             time.Duration     `json:"timeout" validate:"omitempty"`
}

// PreheatResponse defines the response parameters for preheating.
type PreheatResponse struct {
	SuccessTasks       []*PreheatSuccessTask `json:"success_tasks"`
	FailureTasks       []*PreheatFailureTask `json:"failure_tasks"`
	SchedulerClusterID uint                  `json:"scheduler_cluster_id"`
}

// PreheatSuccessTask defines the response parameters for preheating successfully.
type PreheatSuccessTask struct {
	URL      string `json:"url"`
	Hostname string `json:"hostname"`
	IP       string `json:"ip"`
}

// PreheatFailureTask defines the response parameters for preheating failed.
type PreheatFailureTask struct {
	URL         string `json:"url"`
	Hostname    string `json:"hostname"`
	IP          string `json:"ip"`
	Description string `json:"description"`
}

// GetTaskRequest defines the request parameters for getting task.
type GetTaskRequest struct {
	TaskID  string        `json:"task_id" validate:"required"`
	Timeout time.Duration `json:"timeout" validate:"omitempty"`
}

// GetTaskResponse defines the response parameters for getting task.
type GetTaskResponse struct {
	Peers              []*Peer `json:"peers"`
	SchedulerClusterID uint    `json:"scheduler_cluster_id"`
}

// Peer represents the peer information.
type Peer struct {
	ID        string    `json:"id"`
	Hostname  string    `json:"hostname"`
	IP        string    `json:"ip"`
	HostType  string    `json:"host_type"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// DeleteTaskRequest defines the request parameters for deleting task.
type DeleteTaskRequest struct {
	TaskID  string        `json:"task_id" validate:"required"`
	Timeout time.Duration `json:"timeout" validate:"omitempty"`
}

// DeleteTaskResponse defines the response parameters for deleting task.
type DeleteTaskResponse struct {
	SuccessTasks       []*DeleteSuccessTask `json:"success_tasks"`
	FailureTasks       []*DeleteFailureTask `json:"failure_tasks"`
	SchedulerClusterID uint                 `json:"scheduler_cluster_id"`
}

// DeleteSuccessTask defines the response parameters for deleting peer successfully.
type DeleteSuccessTask struct {
	Hostname string `json:"hostname"`
	IP       string `json:"ip"`
	HostType string `json:"host_type"`
}

// DeleteFailureTask defines the response parameters for deleting peer failed.
type DeleteFailureTask struct {
	Hostname    string `json:"hostname"`
	IP          string `json:"ip"`
	HostType    string `json:"host_type"`
	Description string `json:"description"`
}
