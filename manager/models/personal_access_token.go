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

import "time"

const (
	// PersonalAccessTokenStateActive represents the personal access token whose state is active.
	PersonalAccessTokenStateActive = "active"

	// PersonalAccessTokenStateInactive represents the personal access token whose state is inactive.
	PersonalAccessTokenStateInactive = "inactive"
)

type PersonalAccessToken struct {
	BaseModel
	Name      string    `gorm:"column:name;type:varchar(256);index:uk_personal_access_token_name,unique;not null;comment:name" json:"name"`
	BIO       string    `gorm:"column:bio;type:varchar(1024);comment:biography" json:"bio"`
	Token     string    `gorm:"column:token;type:varchar(256);index:uk_personal_access_token,unique;not null;comment:access token" json:"token"`
	Scopes    Array     `gorm:"column:scopes;not null;comment:scopes flags" json:"scopes"`
	State     string    `gorm:"column:state;type:varchar(256);default:'inactive';comment:service state" json:"state"`
	ExpiredAt time.Time `gorm:"column:expired_at;type:timestamp;default:current_timestamp;not null;comment:expired at" json:"expired_at"`
	UserID    uint      `gorm:"column:user_id;comment:user id" json:"user_id"`
	User      User      `json:"user"`
}
