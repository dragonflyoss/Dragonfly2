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

type SchedulerClusterParams struct {
	ID uint `uri:"id" binding:"required"`
}

type AddSchedulerToSchedulerClusterParams struct {
	ID          uint `uri:"id" binding:"required"`
	SchedulerID uint `uri:"scheduler_id" binding:"required"`
}

type CreateSchedulerClusterRequest struct {
	Name              string                        `json:"name" binding:"required"`
	BIO               string                        `json:"bio" binding:"omitempty"`
	Config            *SchedulerClusterConfig       `json:"config" binding:"required"`
	ClientConfig      *SchedulerClusterClientConfig `json:"client_config" binding:"required"`
	Scopes            *SchedulerClusterScopes       `json:"scopes" binding:"omitempty"`
	IsDefault         bool                          `json:"is_default" binding:"omitempty"`
	SeedPeerClusterID uint                          `json:"seed_peer_cluster_id" binding:"omitempty"`
}

type UpdateSchedulerClusterRequest struct {
	Name              string                        `json:"name" binding:"omitempty"`
	BIO               string                        `json:"bio" binding:"omitempty"`
	Config            *SchedulerClusterConfig       `json:"config" binding:"omitempty"`
	ClientConfig      *SchedulerClusterClientConfig `json:"client_config" binding:"omitempty"`
	Scopes            *SchedulerClusterScopes       `json:"scopes" binding:"omitempty"`
	IsDefault         bool                          `json:"is_default" binding:"omitempty"`
	SeedPeerClusterID uint                          `json:"seed_peer_cluster_id" binding:"omitempty"`
}

type GetSchedulerClustersQuery struct {
	Name    string `form:"name" binding:"omitempty"`
	Page    int    `form:"page" binding:"omitempty,gte=1"`
	PerPage int    `form:"per_page" binding:"omitempty,gte=1,lte=10000000"`
}

type SchedulerClusterConfig struct {
	CandidateParentLimit uint32 `yaml:"candidateParentLimit" mapstructure:"candidateParentLimit" json:"candidate_parent_limit" binding:"omitempty,gte=1,lte=20"`
	FilterParentLimit    uint32 `yaml:"filterParentLimit" mapstructure:"filterParentLimit" json:"filter_parent_limit" binding:"omitempty,gte=10,lte=1000"`
}

type SchedulerClusterClientConfig struct {
	LoadLimit uint32 `yaml:"loadLimit" mapstructure:"loadLimit" json:"load_limit" binding:"omitempty,gte=1,lte=2000"`
}

type SchedulerClusterScopes struct {
	IDC      string   `yaml:"idc" mapstructure:"idc" json:"idc" binding:"omitempty"`
	Location string   `yaml:"location" mapstructure:"location" json:"location" binding:"omitempty"`
	CIDRs    []string `yaml:"cidrs" mapstructure:"cidrs" json:"cidrs" binding:"omitempty"`
}
