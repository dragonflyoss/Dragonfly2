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

type SeedPeerParams struct {
	ID uint `uri:"id" binding:"required"`
}

type CreateSeedPeerRequest struct {
	Hostname          string `json:"host_name" binding:"required"`
	Type              string `json:"type" binding:"required,oneof=super strong weak"`
	IDC               string `json:"idc" binding:"omitempty"`
	Location          string `json:"location" binding:"omitempty"`
	IP                string `json:"ip" binding:"required"`
	Port              int32  `json:"port" binding:"required"`
	DownloadPort      int32  `json:"download_port" binding:"required"`
	ObjectStoragePort int32  `json:"object_storage_port" binding:"omitempty"`
	SeedPeerClusterID uint   `json:"seed_peer_cluster_id" binding:"required"`
}

type UpdateSeedPeerRequest struct {
	Type              string `json:"type" binding:"omitempty,oneof=super strong weak"`
	IDC               string `json:"idc" binding:"omitempty"`
	Location          string `json:"location" binding:"omitempty"`
	IP                string `json:"ip" binding:"omitempty"`
	Port              int32  `json:"port" binding:"omitempty"`
	DownloadPort      int32  `json:"download_port" binding:"omitempty"`
	ObjectStoragePort int32  `json:"object_storage_port" binding:"omitempty"`
	SeedPeerClusterID uint   `json:"seed_peer_cluster_id" binding:"omitempty"`
}

type GetSeedPeersQuery struct {
	Hostname          string `form:"host_name" binding:"omitempty"`
	Type              string `form:"type" binding:"omitempty,oneof=super strong weak"`
	IDC               string `form:"idc" binding:"omitempty"`
	Location          string `form:"location" binding:"omitempty"`
	IP                string `form:"ip" binding:"omitempty"`
	Port              int32  `form:"port" binding:"omitempty"`
	DownloadPort      int32  `form:"download_port" binding:"omitempty"`
	ObjectStoragePort int32  `form:"object_storage_port" binding:"omitempty"`
	SeedPeerClusterID uint   `form:"seed_peer_cluster_id" binding:"omitempty"`
	Page              int    `form:"page" binding:"omitempty,gte=1"`
	PerPage           int    `form:"per_page" binding:"omitempty,gte=1,lte=10000000"`
	State             string `form:"state" binding:"omitempty,oneof=active inactive"`
}
