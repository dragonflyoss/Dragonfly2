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

type CasbinRule struct {
	ID    uint   `gorm:"primaryKey;autoIncrement;comment:id"`
	Ptype string `gorm:"type:varchar(100);default:NULL;uniqueIndex:uk_casbin_rule;comment:policy type"`
	V0    string `gorm:"type:varchar(100);default:NULL;uniqueIndex:uk_casbin_rule;comment:v0"`
	V1    string `gorm:"type:varchar(100);default:NULL;uniqueIndex:uk_casbin_rule;comment:v1"`
	V2    string `gorm:"type:varchar(100);default:NULL;uniqueIndex:uk_casbin_rule;comment:v2"`
	V3    string `gorm:"type:varchar(100);default:NULL;uniqueIndex:uk_casbin_rule;comment:v3"`
	V4    string `gorm:"type:varchar(100);default:NULL;uniqueIndex:uk_casbin_rule;comment:v4"`
	V5    string `gorm:"type:varchar(100);default:NULL;uniqueIndex:uk_casbin_rule;comment:v5"`
}
