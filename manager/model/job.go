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

type Job struct {
	Model
	Name   string  `gorm:"column:name;type:varchar(256);index:uk_cdn_cluster_name,unique;not null;comment:name" json:"name"`
	BIO    string  `gorm:"column:bio;type:varchar(1024);comment:biography" json:"bio"`
	TaskID string  `gorm:"column:task_id;type:varchar(256);not null;comment:task id" json:"task_id"`
	Type   string  `gorm:"column:type;type:varchar(256);comment:type" json:"type"`
	Status string  `gorm:"column:status;type:varchar(256);default:'PENDING';comment:service status" json:"status"`
	Body   JSONMap `gorm:"column:body;not null;comment:task request body" json:"body"`
	UserID uint    `gorm:"comment:user id" json:"user_id"`
	User   User    `json:"-"`
}
