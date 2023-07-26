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

type Job struct {
	BaseModel
	TaskID            string             `gorm:"column:task_id;type:varchar(256);not null;comment:task id" json:"task_id"`
	BIO               string             `gorm:"column:bio;type:varchar(1024);comment:biography" json:"bio"`
	Type              string             `gorm:"column:type;type:varchar(256);comment:type" json:"type"`
	State             string             `gorm:"column:state;type:varchar(256);not null;default:'PENDING';comment:service state" json:"state"`
	Args              JSONMap            `gorm:"column:args;not null;comment:task request args" json:"args"`
	Result            JSONMap            `gorm:"column:result;comment:task result" json:"result"`
	UserID            uint               `gorm:"column:user_id;comment:user id" json:"user_id"`
	User              User               `json:"user"`
	SeedPeerClusters  []SeedPeerCluster  `gorm:"many2many:job_seed_peer_cluster;" json:"seed_peer_clusters"`
	SchedulerClusters []SchedulerCluster `gorm:"many2many:job_scheduler_cluster;" json:"scheduler_clusters"`
}
