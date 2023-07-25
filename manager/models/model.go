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

package models

const (
	// ModelVersionStateActive represents the model version
	// whose state is active and the model version currently being used.
	ModelVersionStateActive = "active"

	// ModelVersionStateInactive represents the model version
	// whose state is inactive and the model version currently being not used.
	ModelVersionStateInactive = "inactive"

	// ModelTypeGNN represents the model type is GNN.
	ModelTypeGNN = "gnn"

	// ModelTypeMLP represents the model type is MLP.
	ModelTypeMLP = "mlp"
)

// TODO(Gaius) Add regression analysis parameters.
type Model struct {
	BaseModel
	Name        string    `gorm:"column:name;type:varchar(256);not null;comment:name" json:"name"`
	Type        string    `gorm:"column:type;type:varchar(256);index:uk_model,unique;not null;comment:type" json:"type"`
	BIO         string    `gorm:"column:bio;type:varchar(1024);comment:biography" json:"bio"`
	Version     string    `gorm:"column:version;type:varchar(256);index:uk_model,unique;not null;comment:model version" json:"version"`
	State       string    `gorm:"column:state;type:varchar(256);default:'inactive';comment:model state" json:"state"`
	Evaluation  JSONMap   `gorm:"column:evaluation;comment:evaluation metrics" json:"evaluation"`
	SchedulerID uint      `gorm:"index:uk_model,unique;not null;comment:scheduler id" json:"scheduler_id"`
	Scheduler   Scheduler `json:"scheduler"`
}
