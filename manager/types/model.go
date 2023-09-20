/*
 *     Copyright 2023 The Dragonfly Authors
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
	"fmt"
)

const (
	// ModelFileName is model file name.
	ModelFileName = "model.graphdef"

	// ModelConfigFileName is model config file name.
	ModelConfigFileName = "config.pbtxt"

	// GNNModelNameSuffix is suffix of GNN model name.
	GNNModelNameSuffix = "gnn"

	// MLPModelNameSuffix is suffix of MLP model name.
	MLPModelNameSuffix = "mlp"

	// DefaultTritonPlatform is default triton backend configuration.
	DefaultTritonPlatform = "tensorrt_plan"
)

type ModelParams struct {
	ID uint `uri:"id" binding:"required"`
}

type UpdateModelRequest struct {
	BIO   string `json:"BIO" binding:"omitempty"`
	State string `json:"state" binding:"omitempty,oneof=active inactive"`
}

type GetModelsQuery struct {
	Name        string `json:"name" binding:"omitempty"`
	Type        string `json:"type" binding:"omitempty"`
	Version     string `json:"version"  binding:"omitempty"`
	SchedulerID uint   `json:"scheduler_id" binding:"omitempty"`
	Page        int    `form:"page" binding:"omitempty,gte=1"`
	PerPage     int    `form:"per_page" binding:"omitempty,gte=1,lte=10000000"`
}

type ModelEvaluation struct {
	Recall    float64 `json:"recall" binding:"omitempty,gte=0,lte=1"`
	Precision float64 `json:"precision" binding:"omitempty,gte=0,lte=1"`
	F1Score   float64 `json:"f1_score" binding:"omitempty,gte=0,lte=1"`
	MSE       float64 `json:"mse" binding:"omitempty,gte=0"`
	MAE       float64 `json:"mae" binding:"omitempty,gte=0"`
}

// MakeObjectKeyOfModelFile returns object key of model file.
func MakeObjectKeyOfModelFile(id string, version int) string {
	return fmt.Sprintf("%s/%s/%s", id, fmt.Sprint(version), ModelFileName)
}

// MakeObjectKeyOfModelConfigFile returns object key of model config file.
func MakeObjectKeyOfModelConfigFile(id string) string {
	return fmt.Sprintf("%s/%s", id, ModelConfigFileName)
}
