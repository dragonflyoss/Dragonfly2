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

type CallSystem struct {
	Model
	Name              string             `gorm:"column:name;type:varchar(256);index:uk_callsystem_name,unique;not null;comment:name" json:"name"`
	LimitFrequency    string             `gorm:"column:limit_frequency;type:varchar(1024);comment:limit_frequency" json:"limit_frequency"`
	URLRegex          string             `gorm:"column:url_regexs;not null;comment:url_regexs" json:"url_regexs"`
	IsEnable          bool               `gorm:"column:is_enable;not null;default:true;comment:enable callsystem" json:"is_enable"`
	State             string             `gorm:"column:state;type:varchar(256);default:'enable';comment:state" json:"state"`
	SchedulerClusters []SchedulerCluster `json:"-"`
	CDNClusters       []CDNCluster       `json:"-"`
}
