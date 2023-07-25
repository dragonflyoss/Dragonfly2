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

type Config struct {
	BaseModel
	Name   string `gorm:"column:name;type:varchar(256);index:uk_config_name,unique;not null;comment:config name" json:"name"`
	Value  string `gorm:"column:value;type:varchar(1024);not null;comment:config value" json:"value"`
	BIO    string `gorm:"column:bio;type:varchar(1024);comment:biography" json:"bio"`
	UserID uint   `gorm:"comment:user id" json:"user_id"`
	User   User   `json:"user"`
}
