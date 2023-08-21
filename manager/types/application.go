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

type CreateApplicationRequest struct {
	Name     string          `json:"name" binding:"required"`
	URL      string          `json:"url" binding:"required"`
	BIO      string          `json:"bio" binding:"omitempty"`
	Priority *PriorityConfig `json:"priority" binding:"required"`
	UserID   uint            `json:"user_id" binding:"required"`
}

type UpdateApplicationRequest struct {
	Name     string          `json:"name" binding:"omitempty"`
	URL      string          `json:"url" binding:"omitempty"`
	BIO      string          `json:"bio" binding:"omitempty"`
	Priority *PriorityConfig `json:"priority" binding:"omitempty"`
	UserID   uint            `json:"user_id" binding:"required"`
}

type GetApplicationsQuery struct {
	Name    string `form:"name" binding:"omitempty"`
	Page    int    `form:"page" binding:"omitempty,gte=1"`
	PerPage int    `form:"per_page" binding:"omitempty,gte=1,lte=10000000"`
}

type PriorityConfig struct {
	Value *int                `yaml:"value" mapstructure:"value" json:"value" binding:"required,gte=0,lte=20"`
	URLs  []URLPriorityConfig `yaml:"urls" mapstructure:"urls" json:"urls" binding:"omitempty"`
}

type URLPriorityConfig struct {
	Regex string `yaml:"regex" mapstructure:"regex" json:"regex" binding:"required"`
	Value int    `yaml:"value" mapstructure:"value" json:"value" binding:"required,gte=0,lte=20"`
}
