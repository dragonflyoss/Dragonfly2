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

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/schema"
	"gorm.io/plugin/soft_delete"
)

type BaseModel struct {
	ID        uint                  `gorm:"primarykey;comment:id" json:"id"`
	CreatedAt time.Time             `gorm:"column:created_at;type:timestamp;default:current_timestamp" json:"created_at"`
	UpdatedAt time.Time             `gorm:"column:updated_at;type:timestamp;default:current_timestamp" json:"updated_at"`
	IsDel     soft_delete.DeletedAt `gorm:"softDelete:flag;comment:soft delete flag" json:"is_del"`
}

func Paginate(page, perPage int) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		offset := (page - 1) * perPage
		return db.Offset(offset).Limit(perPage)
	}
}

type JSONMap map[string]any

func (m JSONMap) Value() (driver.Value, error) {
	if m == nil {
		return nil, nil
	}
	ba, err := m.MarshalJSON()
	return string(ba), err
}

func (m *JSONMap) Scan(val any) error {
	var ba []byte
	switch v := val.(type) {
	case []byte:
		ba = v
	case string:
		ba = []byte(v)
	default:
		return errors.New(fmt.Sprint("Failed to unmarshal JSONB value:", val))
	}
	t := map[string]any{}
	err := json.Unmarshal(ba, &t)
	*m = JSONMap(t)
	return err
}

func (m JSONMap) MarshalJSON() ([]byte, error) {
	if m == nil {
		return []byte("null"), nil
	}
	t := (map[string]any)(m)
	return json.Marshal(t)
}

func (m *JSONMap) UnmarshalJSON(b []byte) error {
	t := map[string]any{}
	err := json.Unmarshal(b, &t)
	*m = JSONMap(t)
	return err
}

func (m JSONMap) GormDataType() string {
	return "jsonmap"
}

func (JSONMap) GormDBDataType(db *gorm.DB, field *schema.Field) string {
	return "text"
}

type Array []string

func (a Array) Value() (driver.Value, error) {
	if a == nil {
		return nil, nil
	}
	ba, err := a.MarshalJSON()
	return string(ba), err
}

func (a *Array) Scan(val any) error {
	var ba []byte
	switch v := val.(type) {
	case []byte:
		ba = v
	case string:
		ba = []byte(v)
	default:
		return errors.New(fmt.Sprint("Failed to unmarshal JSONB value:", val))
	}
	t := []string{}
	err := json.Unmarshal(ba, &t)
	*a = Array(t)
	return err
}

func (a Array) MarshalJSON() ([]byte, error) {
	if a == nil {
		return []byte("null"), nil
	}
	t := ([]string)(a)
	return json.Marshal(t)
}

func (a *Array) UnmarshalJSON(b []byte) error {
	t := []string{}
	err := json.Unmarshal(b, &t)
	*a = Array(t)
	return err
}

func (Array) GormDataType() string {
	return "array"
}

func (Array) GormDBDataType(db *gorm.DB, field *schema.Field) string {
	return "text"
}
