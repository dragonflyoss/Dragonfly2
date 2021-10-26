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
	DisplayName       string             `gorm:"column:display_name;type:varchar(1024);comment:display_name" json:"display_name"`
	LimitRate         string             `gorm:"column:limit_rate;type:varchar(1024);comment:limit_rate" json:"limit_rate"`
	URLRegexs         JSONList           `gorm:"column:url_regexs;not null;comment:url_regexs" json:"url_regexs"`
	TermOfValidity    string             `gorm:"column:term_of_validity;type:varchar(1024);comment:term_of_validity" json:"term_of_validity"`
	IsEnable          bool               `gorm:"column:is_enable;not null;default:true;comment:enable callsystem" json:"is_enable"`
	SchedulerClusters []SchedulerCluster `json:"-"`
}
