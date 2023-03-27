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

type Oauth struct {
	BaseModel
	Name         string `gorm:"column:name;type:varchar(256);index:uk_oauth2_name,unique;not null;comment:oauth2 name" json:"name"`
	BIO          string `gorm:"column:bio;type:varchar(1024);comment:biography" json:"bio"`
	ClientID     string `gorm:"column:client_id;type:varchar(256);index:uk_oauth2_client_id,unique;not null;comment:client id for oauth2" json:"client_id"`
	ClientSecret string `gorm:"column:client_secret;type:varchar(1024);not null;comment:client secret for oauth2" json:"client_secret"`
	RedirectURL  string `gorm:"column:redirect_url;type:varchar(1024);comment:authorization callback url" json:"redirect_url"`
}
