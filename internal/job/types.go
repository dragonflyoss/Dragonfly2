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

import "d7y.io/dragonfly/v2/scheduler/resource"

type PreheatRequest struct {
	URL                 string            `json:"url" validate:"required,url"`
	Tag                 string            `json:"tag" validate:"omitempty"`
	Digest              string            `json:"digest" validate:"omitempty"`
	FilteredQueryParams string            `json:"filtered_query_params" validate:"omitempty"`
	Headers             map[string]string `json:"headers" validate:"omitempty"`
	Application         string            `json:"application" validate:"omitempty"`
	Priority            int32             `json:"priority" validate:"omitempty"`
	PieceLength         uint32            `json:"pieceLength" validate:"omitempty"`
}

type PreheatResponse struct {
	TaskID string `json:"taskID"`
}

// ListTasksRequest defines the request parameters for listing tasks.
type ListTasksRequest struct {
	TaskID  string `json:"task_id" validate:"required"`
	Page    int    `json:"page" validate:"required"`
	PerPage int    `json:"per_page" validate:"required"`
}

// ListTasksResponse defines the response parameters for listing tasks.
type ListTasksResponse struct {
	Peers []*resource.Peer `json:"peers"`
	Page  int              `json:"page"`
	Total int              `json:"total"`
}

// DeleteTaskRequest defines the request parameters for deleting task.
type DeleteTaskRequest struct {
	TaskID string `json:"task_id" validate:"required"`
}

// TaskInfo includes information about a task along with peer details and a description.
type TaskInfo struct {
	Task *resource.Task `json:"task"`
	Peer *resource.Peer `json:"peer"`
	Desc string         `json:"desc"`
}

// DeleteTaskResponse represents the response after attempting to delete tasks,
// categorizing them into successfully and unsuccessfully deleted.
type DeleteTaskResponse struct {
	SuccessTasks []*TaskInfo `json:"success_tasks"`
	FailureTasks []*TaskInfo `json:"failure_tasks"`
}
