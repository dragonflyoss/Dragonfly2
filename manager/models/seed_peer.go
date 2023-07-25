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

package models

const (
	// SeedPeerStateActive represents the seed peer whose state is active.
	SeedPeerStateActive = "active"

	// SeedPeerStateInactive represents the seed peer whose state is inactive.
	SeedPeerStateInactive = "inactive"
)

type SeedPeer struct {
	BaseModel
	Hostname          string          `gorm:"column:host_name;type:varchar(256);index:uk_seed_peer,unique;not null;comment:hostname" json:"host_name"`
	Type              string          `gorm:"column:type;type:varchar(256);comment:type" json:"type"`
	IDC               string          `gorm:"column:idc;type:varchar(1024);comment:internet data center" json:"idc"`
	Location          string          `gorm:"column:location;type:varchar(1024);comment:location" json:"location"`
	IP                string          `gorm:"column:ip;type:varchar(256);index:uk_seed_peer,unique;not null;comment:ip address" json:"ip"`
	Port              int32           `gorm:"column:port;not null;comment:grpc service listening port" json:"port"`
	DownloadPort      int32           `gorm:"column:download_port;not null;comment:download service listening port" json:"download_port"`
	ObjectStoragePort int32           `gorm:"column:object_storage_port;comment:object storage service listening port" json:"object_storage_port"`
	State             string          `gorm:"column:state;type:varchar(256);default:'inactive';comment:service state" json:"state"`
	SeedPeerClusterID uint            `gorm:"index:uk_seed_peer,unique;not null;comment:seed peer cluster id" json:"seed_peer_cluster_id"`
	SeedPeerCluster   SeedPeerCluster `json:"seed_peer_cluster"`
}
