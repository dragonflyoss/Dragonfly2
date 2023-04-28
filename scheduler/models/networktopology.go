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

type Probes struct {
	AverageRTT int64 `gorm:"column:averageRTT;not null;comment:average round-trip time" json:"averageRTT"`
	CreatedAt  int64 `gorm:"column:createdAt;not null;comment:probe create nanosecond time" json:"createdAt"`
	UpdatedAt  int64 `gorm:"column:updatedAt;not null;comment:probe update nanosecond time" json:"updatedAt"`
}

type DestHost struct {
	Host   string `gorm:"column:host;type:varchar(1024);comment:probe source host" json:"host"`
	Probes Probes `gorm:"column:probes;not null;comment:network information probed to destination host" json:"probes"`
}

type Scheduler struct {
	ID        string     `gorm:"column:id;type:varchar(1024);comment:network topology id" json:"id"`
	Host      string     `gorm:"column:host;type:varchar(1024);comment:probe source host" json:"host"`
	DestHosts []DestHost `gorm:"foreignKey:destination_hosts;" json:"destination_hosts"`
}
