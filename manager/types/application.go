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

const (
	// PriorityLevel0 represents the download task is forbidden,
	// and an error code is returned during the registration.
	PriorityLevel0 = iota

	// PriorityLevel1 represents when the task is downloaded for the first time,
	// allow peers to download from the other peers,
	// but not back-to-source. When the task is not downloaded for
	// the first time, it is scheduled normally.
	PriorityLevel1

	// PriorityLevel2 represents when the task is downloaded for the first time,
	// the normal peer is first to download back-to-source.
	// When the task is not downloaded for the first time, it is scheduled normally.
	PriorityLevel2

	// PriorityLevel3 represents when the task is downloaded for the first time,
	// the weak peer is first triggered to back-to-source.
	// When the task is not downloaded for the first time, it is scheduled normally.
	PriorityLevel3

	// PriorityLevel4 represents when the task is downloaded for the first time,
	// the strong peer is first triggered to back-to-source.
	// When the task is not downloaded for the first time, it is scheduled normally.
	PriorityLevel4

	// PriorityLevel4 represents when the task is downloaded for the first time,
	// the super peer is first triggered to back-to-source.
	// When the task is not downloaded for the first time, it is scheduled normally.
	PriorityLevel5
)

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
	PerPage int    `form:"per_page" binding:"omitempty,gte=1,lte=50"`
}

type PriorityConfig struct {
	Value *int        `yaml:"value" mapstructure:"value" json:"value" binding:"required,gte=0,lte=20"`
	URLs  []URLConfig `yaml:"urls" mapstructure:"urls" json:"urls" binding:"omitempty"`
}

type URLConfig struct {
	Regex string `yaml:"regex" mapstructure:"regex" json:"regex" binding:"required"`
	Value int    `yaml:"value" mapstructure:"value" json:"value" binding:"required,gte=0,lte=20"`
}
