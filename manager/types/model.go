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

import (
	"encoding/json"
	"time"
)

const (
	// ModelIDEvaluator is id of the evaluation model.
	ModelIDEvaluator = "evaluator"
)

type ModelParams struct {
	SchedulerID uint   `uri:"id" binding:"required"`
	ID          string `uri:"model_id" binding:"required"`
}

type CreateModelParams struct {
	SchedulerID uint `uri:"id" binding:"required"`
}

type CreateModelRequest struct {
	ID          string `json:"id" binding:"required,oneof=evaluator"`
	Name        string `json:"name" binding:"required"`
	VersionID   string `json:"version_id" binding:"required"`
	SchedulerID uint   `json:"scheduler_id" binding:"required"`
	Hostname    string `json:"hostname" binding:"required"`
	IP          string `json:"ip" binding:"required"`
}

type GetModelsParams struct {
	SchedulerID uint `uri:"id" binding:"required"`
}

type UpdateModelRequest struct {
	Name      string `json:"name" binding:"omitempty"`
	VersionID string `json:"version_id" binding:"omitempty"`
	Hostname  string `json:"hostname" binding:"omitempty"`
	IP        string `json:"ip" binding:"omitempty"`
}

type Model struct {
	ID          string    `json:"id" binding:"required"`
	Name        string    `json:"name" binding:"required"`
	VersionID   string    `json:"version_id" binding:"required"`
	SchedulerID uint      `json:"scheduler_id" binding:"required"`
	Hostname    string    `json:"hostname" binding:"required"`
	IP          string    `json:"ip" binding:"required"`
	CreatedAt   time.Time `json:"create_at" binding:"required"`
	UpdatedAt   time.Time `json:"updated_at" binding:"required"`
}

func (m *Model) MarshalBinary() ([]byte, error) {
	return json.Marshal(m)
}

func (m *Model) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, m)
}

type ModelVersionParams struct {
	SchedulerID uint   `uri:"id" binding:"required"`
	ModelID     string `uri:"model_id" binding:"required"`
	ID          string `uri:"version_id" binding:"required"`
}

type CreateModelVersionParams struct {
	SchedulerID uint   `uri:"id" binding:"required"`
	ModelID     string `uri:"model_id" binding:"required"`
}

type CreateModelVersionRequest struct {
	Precision float64 `json:"precision" binding:"required"`
	Recall    float64 `json:"recall" binding:"required"`
	Data      []byte  `json:"data" binding:"required"`
}

type GetModelVersionsParams struct {
	SchedulerID uint   `uri:"id" binding:"required"`
	ModelID     string `uri:"model_id" binding:"required"`
}

type UpdateModelVersionRequest struct {
	Precision float64 `json:"precision" binding:"omitempty"`
	Recall    float64 `json:"recall" binding:"omitempty"`
	Data      []byte  `json:"data" binding:"omitempty"`
}

type ModelVersion struct {
	ID        string    `json:"id" binding:"required"`
	Precision float64   `json:"precision" binding:"required"`
	Recall    float64   `json:"recall" binding:"required"`
	Data      []byte    `json:"data" binding:"required"`
	CreatedAt time.Time `json:"create_at" binding:"required"`
	UpdatedAt time.Time `json:"updated_at" binding:"required"`
}

func (mv *ModelVersion) MarshalBinary() ([]byte, error) {
	return json.Marshal(mv)
}

func (mv *ModelVersion) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, mv)
}
