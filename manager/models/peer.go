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

package models

const (
	// PeerStateActive represents the peer whose state is active.
	PeerStateActive = "active"

	// PeerStateInactive represents the peer whose state is inactive.
	PeerStateInactive = "inactive"
)

type Peer struct {
	BaseModel
	Hostname           string           `gorm:"column:host_name;type:varchar(256);index:uk_peer,unique;not null;comment:hostname" json:"host_name"`
	Type               string           `gorm:"column:type;type:varchar(256);index:idx_peer_type;comment:type" json:"type"`
	IDC                string           `gorm:"column:idc;type:varchar(1024);comment:internet data center" json:"idc"`
	Location           string           `gorm:"column:location;type:varchar(1024);comment:location" json:"location"`
	IP                 string           `gorm:"column:ip;type:varchar(256);index:uk_peer,unique;not null;comment:ip address" json:"ip"`
	Port               int32            `gorm:"column:port;not null;comment:grpc service listening port" json:"port"`
	DownloadPort       int32            `gorm:"column:download_port;not null;comment:download service listening port" json:"download_port"`
	ObjectStoragePort  int32            `gorm:"column:object_storage_port;comment:object storage service listening port" json:"object_storage_port"`
	State              string           `gorm:"column:state;type:varchar(256);default:'inactive';comment:service state" json:"state"`
	OS                 string           `gorm:"column:os;type:varchar(256);comment:os" json:"os"`
	Platform           string           `gorm:"column:platform;type:varchar(256);comment:platform" json:"platform"`
	PlatformFamily     string           `gorm:"column:platform_family;type:varchar(256);comment:platform family" json:"platform_family"`
	PlatformVersion    string           `gorm:"column:platform_version;type:varchar(256);comment:platform version" json:"platform_version"`
	KernelVersion      string           `gorm:"column:kernel_version;type:varchar(256);comment:kernel version" json:"kernel_version"`
	GitVersion         string           `gorm:"column:git_version;type:varchar(256);index:idx_peer_git_version;comment:git version" json:"git_version"`
	GitCommit          string           `gorm:"column:git_commit;type:varchar(256);index:idx_peer_git_commit;comment:git commit" json:"git_commit"`
	BuildPlatform      string           `gorm:"column:build_platform;type:varchar(256);comment:build platform" json:"build_platform"`
	SchedulerClusterID uint             `gorm:"index:uk_peer,unique;not null;comment:scheduler cluster id" json:"scheduler_cluster_id"`
	SchedulerCluster   SchedulerCluster `json:"scheduler_cluster"`
}
