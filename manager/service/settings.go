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

package service

import (
	"errors"

	"d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/manager/types"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
)

var AvaliableSettings = []string{
	"server_domain",
}

func (s *rest) CreateSetting(json types.CreateSettingRequest) (*model.Settings, error) {
	if !stringutils.Contains(AvaliableSettings, json.Key) {
		return nil, errors.New("invalid setting key")
	}
	setting := model.Settings{
		Key:   json.Key,
		Value: json.Value,
	}

	if err := s.db.Create(&setting).Error; err != nil {
		return nil, err
	}

	return &setting, nil
}

func (s *rest) DestroySetting(key string) error {
	if err := s.db.Unscoped().Delete(&model.Settings{}, model.Settings{Key: key}).Error; err != nil {
		return err
	}

	return nil
}

func (s *rest) UpdateSetting(key string, json types.UpdateSettingRequest) (*model.Settings, error) {
	setting := model.Settings{}
	if err := s.db.First(&setting, model.Settings{Key: key}).Updates(model.Settings{
		Key:   json.Key,
		Value: json.Value,
	}).Error; err != nil {
		return nil, err
	}

	return &setting, nil
}

func (s *rest) GetSettings(q types.GetSettingsQuery) (*[]model.Settings, error) {
	settings := []model.Settings{}
	if err := s.db.Scopes(model.Paginate(q.Page, q.PerPage)).Find(&settings).Error; err != nil {
		return nil, err
	}

	return &settings, nil
}

func (s *rest) SettingTotalCount() (int64, error) {
	var count int64
	if err := s.db.Model(&model.Settings{}).Count(&count).Error; err != nil {
		return 0, err
	}

	return count, nil
}
