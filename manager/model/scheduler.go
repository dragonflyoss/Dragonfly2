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

package model

const (
	SchedulerStateActive   = "active"
	SchedulerStateInactive = "inactive"
)

type Scheduler struct {
	Model
	HostName           string           `gorm:"column:host_name;type:varchar(256);index:uk_scheduler,unique;not null;comment:hostname" json:"host_name"`
	VIPs               string           `gorm:"column:vips;type:varchar(1024);comment:virtual ip address" json:"vips"`
	IDC                string           `gorm:"column:idc;type:varchar(1024);comment:internet data center" json:"idc"`
	Location           string           `gorm:"column:location;type:varchar(1024);comment:location" json:"location"`
	NetConfig          JSONMap          `gorm:"column:net_config;comment:network configuration" json:"net_config"`
	IP                 string           `gorm:"column:ip;type:varchar(256);not null;comment:ip address" json:"ip"`
	Port               int32            `gorm:"column:port;not null;comment:grpc service listening port" json:"port"`
	State              string           `gorm:"column:state;type:varchar(256);default:'inactive';comment:service state" json:"state"`
	SchedulerClusterID uint             `gorm:"index:uk_scheduler,unique;not null;comment:scheduler cluster id"`
	SchedulerCluster   SchedulerCluster `json:"-"`
}
