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

const (
	UserStateEnabled  = "enable"
	UserStateDisabled = "disable"
)

type User struct {
	BaseModel
	Email             string   `gorm:"column:email;type:varchar(256);index:uk_user_email,unique;not null;comment:email address" json:"email"`
	Name              string   `gorm:"column:name;type:varchar(256);index:uk_user_name,unique;not null;comment:name" json:"name"`
	EncryptedPassword string   `gorm:"column:encrypted_password;size:1024;comment:encrypted password" json:"-"`
	Avatar            string   `gorm:"column:avatar;type:varchar(256);comment:avatar address" json:"avatar"`
	Phone             string   `gorm:"column:phone;type:varchar(256);comment:phone number" json:"phone"`
	PrivateToken      string   `gorm:"column:private_token;type:varchar(256);comment:private token" json:"-"`
	State             string   `gorm:"column:state;type:varchar(256);default:'enable';comment:state" json:"state"`
	Location          string   `gorm:"column:location;type:varchar(256);comment:location" json:"location"`
	BIO               string   `gorm:"column:bio;type:varchar(256);comment:biography" json:"bio"`
	Configs           []Config `json:"configs"`
}
