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
	"context"

	"d7y.io/dragonfly/v2/manager/models"
	"d7y.io/dragonfly/v2/manager/types"
)

func (s *service) CreateConfig(ctx context.Context, json types.CreateConfigRequest) (*models.Config, error) {
	config := models.Config{
		Name:   json.Name,
		Value:  json.Value,
		BIO:    json.BIO,
		UserID: json.UserID,
	}

	if err := s.db.WithContext(ctx).Create(&config).Error; err != nil {
		return nil, err
	}

	return &config, nil
}

func (s *service) DestroyConfig(ctx context.Context, id uint) error {
	config := models.Config{}
	if err := s.db.WithContext(ctx).First(&config, id).Error; err != nil {
		return err
	}

	if err := s.db.WithContext(ctx).Unscoped().Unscoped().Delete(&models.Config{}, id).Error; err != nil {
		return err
	}

	return nil
}

func (s *service) UpdateConfig(ctx context.Context, id uint, json types.UpdateConfigRequest) (*models.Config, error) {
	config := models.Config{}
	if err := s.db.WithContext(ctx).First(&config, id).Updates(models.Config{
		Name:   json.Name,
		Value:  json.Value,
		BIO:    json.BIO,
		UserID: json.UserID,
	}).Error; err != nil {
		return nil, err
	}

	return &config, nil
}

func (s *service) GetConfig(ctx context.Context, id uint) (*models.Config, error) {
	config := models.Config{}
	if err := s.db.WithContext(ctx).First(&config, id).Error; err != nil {
		return nil, err
	}

	return &config, nil
}

func (s *service) GetConfigs(ctx context.Context, q types.GetConfigsQuery) ([]models.Config, int64, error) {
	var count int64
	var configs []models.Config
	if err := s.db.WithContext(ctx).Scopes(models.Paginate(q.Page, q.PerPage)).Where(&models.Config{
		Name:   q.Name,
		Value:  q.Value,
		UserID: q.UserID,
	}).Find(&configs).Limit(-1).Offset(-1).Count(&count).Error; err != nil {
		return nil, 0, err
	}

	return configs, count, nil
}
