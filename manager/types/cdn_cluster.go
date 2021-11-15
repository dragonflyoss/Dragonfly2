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

type CDNClusterParams struct {
	ID uint `uri:"id" binding:"required"`
}

type AddCDNToCDNClusterParams struct {
	ID    uint `uri:"id" binding:"required"`
	CDNID uint `uri:"cdn_id" binding:"required"`
}

type AddSchedulerClusterToCDNClusterParams struct {
	ID                 uint `uri:"id" binding:"required"`
	SchedulerClusterID uint `uri:"scheduler_cluster_id" binding:"required"`
}

type CreateCDNClusterRequest struct {
	Name                string            `json:"name" binding:"required"`
	BIO                 string            `json:"bio" binding:"omitempty"`
	Config              *CDNClusterConfig `json:"config" binding:"required"`
	SecurityGroupDomain string            `json:"security_group_domain" binding:"omitempty"`
}

type UpdateCDNClusterRequest struct {
	Name                string            `json:"name" binding:"omitempty"`
	BIO                 string            `json:"bio" binding:"omitempty"`
	Config              *CDNClusterConfig `json:"config" binding:"omitempty"`
	SecurityGroupDomain string            `json:"security_group_domain" binding:"omitempty"`
}

type GetCDNClustersQuery struct {
	Name    string `form:"name" binding:"omitempty"`
	Page    int    `form:"page" binding:"omitempty,gte=1"`
	PerPage int    `form:"per_page" binding:"omitempty,gte=1,lte=50"`
}

type CDNClusterConfig struct {
	LoadLimit   uint   `yaml:"loadLimit" mapstructure:"loadLimit" json:"load_limit" binding:"omitempty,gte=1,lte=5000"`
	NetTopology string `yaml:"netTopology" mapstructure:"netTopology" json:"net_topology"`
}
