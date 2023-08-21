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

import "time"

type ClusterParams struct {
	ID uint `uri:"id" binding:"required"`
}

type CreateClusterRequest struct {
	Name                   string                        `json:"name" binding:"required"`
	BIO                    string                        `json:"bio" binding:"omitempty"`
	Scopes                 *SchedulerClusterScopes       `json:"scopes" binding:"omitempty"`
	SchedulerClusterConfig *SchedulerClusterConfig       `json:"scheduler_cluster_config" binding:"required"`
	SeedPeerClusterConfig  *SeedPeerClusterConfig        `json:"seed_peer_cluster_config" binding:"required"`
	PeerClusterConfig      *SchedulerClusterClientConfig `json:"peer_cluster_config" binding:"required"`
	IsDefault              bool                          `json:"is_default" binding:"omitempty"`
}

type CreateClusterResponse struct {
	ID                     uint                          `json:"id"`
	Name                   string                        `json:"name"`
	BIO                    string                        `json:"bio"`
	Scopes                 *SchedulerClusterScopes       `json:"scopes"`
	SchedulerClusterID     uint                          `json:"scheduler_cluster_id"`
	SeedPeerClusterID      uint                          `json:"seed_peer_cluster_id"`
	SchedulerClusterConfig *SchedulerClusterConfig       `json:"scheduler_cluster_config"`
	SeedPeerClusterConfig  *SeedPeerClusterConfig        `json:"seed_peer_cluster_config"`
	PeerClusterConfig      *SchedulerClusterClientConfig `json:"peer_cluster_config"`
	CreatedAt              time.Time                     `json:"created_at"`
	UpdatedAt              time.Time                     `json:"updated_at"`
	IsDefault              bool                          `json:"is_default"`
}

type UpdateClusterRequest struct {
	Name                   string                        `json:"name" binding:"omitempty"`
	BIO                    string                        `json:"bio" binding:"omitempty"`
	Scopes                 *SchedulerClusterScopes       `json:"scopes" binding:"omitempty"`
	SchedulerClusterConfig *SchedulerClusterConfig       `json:"scheduler_cluster_config" binding:"omitempty"`
	SeedPeerClusterConfig  *SeedPeerClusterConfig        `json:"seed_peer_cluster_config" binding:"omitempty"`
	PeerClusterConfig      *SchedulerClusterClientConfig `json:"peer_cluster_config" binding:"omitempty"`
	IsDefault              bool                          `json:"is_default" binding:"omitempty"`
}

type UpdateClusterResponse struct {
	ID                     uint                          `json:"id"`
	Name                   string                        `json:"name"`
	BIO                    string                        `json:"bio"`
	Scopes                 *SchedulerClusterScopes       `json:"scopes"`
	SchedulerClusterID     uint                          `json:"scheduler_cluster_id"`
	SeedPeerClusterID      uint                          `json:"seed_peer_cluster_id"`
	SchedulerClusterConfig *SchedulerClusterConfig       `json:"scheduler_cluster_config"`
	SeedPeerClusterConfig  *SeedPeerClusterConfig        `json:"seed_peer_cluster_config"`
	PeerClusterConfig      *SchedulerClusterClientConfig `json:"peer_cluster_config"`
	CreatedAt              time.Time                     `json:"created_at"`
	UpdatedAt              time.Time                     `json:"updated_at"`
	IsDefault              bool                          `json:"is_default"`
}

type GetClusterResponse struct {
	ID                     uint                          `json:"id"`
	Name                   string                        `json:"name"`
	BIO                    string                        `json:"bio"`
	Scopes                 *SchedulerClusterScopes       `json:"scopes"`
	SchedulerClusterID     uint                          `json:"scheduler_cluster_id"`
	SeedPeerClusterID      uint                          `json:"seed_peer_cluster_id"`
	SchedulerClusterConfig *SchedulerClusterConfig       `json:"scheduler_cluster_config"`
	SeedPeerClusterConfig  *SeedPeerClusterConfig        `json:"seed_peer_cluster_config"`
	PeerClusterConfig      *SchedulerClusterClientConfig `json:"peer_cluster_config"`
	CreatedAt              time.Time                     `json:"created_at"`
	UpdatedAt              time.Time                     `json:"updated_at"`
	IsDefault              bool                          `json:"is_default"`
}

type GetClustersQuery struct {
	Name    string `form:"name" binding:"omitempty"`
	Page    int    `form:"page" binding:"omitempty,gte=1"`
	PerPage int    `form:"per_page" binding:"omitempty,gte=1,lte=10000000"`
}
