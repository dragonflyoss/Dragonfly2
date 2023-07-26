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

const (
	// SchedulerStateActive represents the scheduler whose state is active.
	SchedulerStateActive = "active"

	// SchedulerStateInactive represents the scheduler whose state is inactive.
	SchedulerStateInactive = "inactive"
)

type Scheduler struct {
	BaseModel
	Hostname           string           `gorm:"column:host_name;type:varchar(256);index:uk_scheduler,unique;not null;comment:hostname" json:"host_name"`
	IDC                string           `gorm:"column:idc;type:varchar(1024);comment:internet data center" json:"idc"`
	Location           string           `gorm:"column:location;type:varchar(1024);comment:location" json:"location"`
	IP                 string           `gorm:"column:ip;type:varchar(256);index:uk_scheduler,unique;not null;comment:ip address" json:"ip"`
	Port               int32            `gorm:"column:port;not null;comment:grpc service listening port" json:"port"`
	State              string           `gorm:"column:state;type:varchar(256);default:'inactive';comment:service state" json:"state"`
	Features           Array            `gorm:"column:features;comment:feature flags" json:"features"`
	SchedulerClusterID uint             `gorm:"index:uk_scheduler,unique;not null;comment:scheduler cluster id"  json:"scheduler_cluster_id"`
	SchedulerCluster   SchedulerCluster `json:"scheduler_cluster"`
	Models             []Model          `json:"models"`
}
