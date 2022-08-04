/*
 *     Copyright 2022 The Dragonfly Authors
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

type ModelParams struct {
	SchedulerClusterID string `uri:"scheduler_cluster_id" binding:"required"`
	SchedulerID        string `uri:"scheduler_id" binding:"required"`
	ID                 string `uri:"id" binding:"required"`
}

type Model struct {
	ID        string `json:"id" binding:"required"`
	VersionID string `json:"versionID" binding:"required"`
}

type ModelVersionParams struct {
	SchedulerClusterID string `uri:"scheduler_cluster_id" binding:"required"`
	SchedulerID        string `uri:"scheduler_id" binding:"required"`
	ModelID            string `uri:"model_id" binding:"required"`
	ID                 string `uri:"id" binding:"required"`
}

type Version struct {
	ID        string  `json:"id" binding:"required"`
	Precision float64 `json:"precision" binding:"required"`
	Recall    float64 `json:"recall" binding:"required"`
	Data      []byte  `json:"data" binding:"required"`
}
