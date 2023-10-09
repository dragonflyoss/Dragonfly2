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

type PeerParams struct {
	ID uint `uri:"id" binding:"required"`
}

type CreatePeerRequest struct {
	Hostname           string `json:"host_name" binding:"required"`
	Type               string `json:"type" binding:"required,oneof=super strong weak normal"`
	IDC                string `json:"idc" binding:"omitempty"`
	Location           string `json:"location" binding:"omitempty"`
	IP                 string `json:"ip" binding:"required"`
	Port               int32  `json:"port" binding:"required"`
	DownloadPort       int32  `json:"download_port" binding:"required"`
	ObjectStoragePort  int32  `json:"object_storage_port" binding:"omitempty"`
	State              string `json:"state" binding:"omitempty,oneof=active inactive"`
	OS                 string `json:"os" binding:"omitempty"`
	Platform           string `json:"platform" binding:"omitempty"`
	PlatformFamily     string `json:"platform_family" binding:"omitempty"`
	PlatformVersion    string `json:"platform_version" binding:"omitempty"`
	KernelVersion      string `json:"kernel_version" binding:"omitempty"`
	GitVersion         string `json:"git_version" binding:"omitempty"`
	GitCommit          string `json:"git_commit" binding:"omitempty"`
	BuildPlatform      string `json:"build_platform" binding:"omitempty"`
	SchedulerClusterID uint   `json:"scheduler_cluster_id" binding:"required"`
}

type GetPeersQuery struct {
	Hostname           string `form:"host_name" binding:"omitempty"`
	Type               string `form:"type" binding:"omitempty,oneof=super strong weak"`
	IDC                string `form:"idc" binding:"omitempty"`
	Location           string `form:"location" binding:"omitempty"`
	IP                 string `form:"ip" binding:"omitempty"`
	Port               int32  `form:"port" binding:"omitempty"`
	DownloadPort       int32  `form:"download_port" binding:"omitempty"`
	ObjectStoragePort  int32  `form:"object_storage_port" binding:"omitempty"`
	State              string `form:"state" binding:"omitempty,oneof=active inactive"`
	OS                 string `form:"os" binding:"omitempty"`
	Platform           string `form:"platform" binding:"omitempty"`
	PlatformFamily     string `form:"platform_family" binding:"omitempty"`
	PlatformVersion    string `form:"platform_version" binding:"omitempty"`
	KernelVersion      string `form:"kernel_version" binding:"omitempty"`
	GitVersion         string `form:"git_version" binding:"omitempty"`
	GitCommit          string `form:"git_commit" binding:"omitempty"`
	BuildPlatform      string `form:"build_platform" binding:"omitempty"`
	SchedulerClusterID uint   `form:"scheduler_cluster_id" binding:"omitempty"`
	Page               int    `form:"page" binding:"omitempty,gte=1"`
	PerPage            int    `form:"per_page" binding:"omitempty,gte=1,lte=10000000"`
}
