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

type Model struct {
	ID        uint                  `gorm:"primarykey;comment:id" json:"id"`
	CreatedAt time.Time             `gorm:"column:created_at;type:timestamp;comment:created timestamp" json:"created_at"`
	UpdatedAt time.Time             `gorm:"column:updated_at;type:timestamp;comment:updated timestamp" json:"updated_at"`
	IsDel     soft_delete.DeletedAt `gorm:"softDelete:flag;comment:soft delete flag" json:"-"`
}

func Paginate(page, perPage int) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		offset := (page - 1) * perPage
		return db.Offset(offset).Limit(perPage)
	}
}

type JSONMap map[string]interface{}
type JSONList []string

func (m JSONMap) Value() (driver.Value, error) {
	if m == nil {
		return nil, nil
	}
	ba, err := m.MarshalJSON()
	return string(ba), err
}

func (l JSONList) Value() (driver.Value, error) {
	if l == nil {
		return nil, nil
	}
	ba, err := l.MarshalJSON()
	return string(ba), err
}

func (m *JSONMap) Scan(val interface{}) error {
	var ba []byte
	switch v := val.(type) {
	case []byte:
		ba = v
	case string:
		ba = []byte(v)
	default:
		return errors.New(fmt.Sprint("Failed to unmarshal JSONB value:", val))
	}
	t := map[string]interface{}{}
	err := json.Unmarshal(ba, &t)
	*m = JSONMap(t)
	return err
}

func (l *JSONList) Scan(val interface{}) error {
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
	*l = JSONList(t)
	return err
}

func (m JSONMap) MarshalJSON() ([]byte, error) {
	if m == nil {
		return []byte("null"), nil
	}
	t := (map[string]interface{})(m)
	return json.Marshal(t)
}

func (l JSONList) MarshalJSON() ([]byte, error) {
	if l == nil {
		return []byte("null"), nil
	}
	t := ([]string)(l)
	return json.Marshal(t)
}

func (m *JSONMap) UnmarshalJSON(b []byte) error {
	t := map[string]interface{}{}
	err := json.Unmarshal(b, &t)
	*m = JSONMap(t)
	return err
}

func (l *JSONList) UnmarshalJSON(b []byte) error {
	t := []string{}
	err := json.Unmarshal(b, &t)
	*l = JSONList(t)
	return err
}

func (m JSONMap) GormDataType() string {
	return "jsonmap"
}

func (JSONMap) GormDBDataType(db *gorm.DB, field *schema.Field) string {
	return "text"
}

func (l JSONList) GormDataType() string {
	return "jsonlist"
}

func (JSONList) GormDBDataType(db *gorm.DB, field *schema.Field) string {
	return "text"
}
