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

package models

type SchedulerCluster struct {
	BaseModel
	Name             string            `gorm:"column:name;type:varchar(256);index:uk_scheduler_cluster_name,unique;not null;comment:name" json:"name"`
	BIO              string            `gorm:"column:bio;type:varchar(1024);comment:biography" json:"bio"`
	Config           JSONMap           `gorm:"column:config;not null;comment:configuration" json:"config"`
	ClientConfig     JSONMap           `gorm:"column:client_config;not null;comment:client configuration" json:"client_config"`
	Scopes           JSONMap           `gorm:"column:scopes;comment:match scopes" json:"scopes"`
	IsDefault        bool              `gorm:"column:is_default;not null;default:false;comment:default scheduler cluster" json:"is_default"`
	SeedPeerClusters []SeedPeerCluster `gorm:"many2many:seed_peer_cluster_scheduler_cluster;" json:"seed_peer_clusters"`
	Schedulers       []Scheduler       `json:"schedulers"`
	Peers            []Peer            `json:"peers"`
	Jobs             []Job             `gorm:"many2many:job_scheduler_cluster;" json:"jobs"`
}
