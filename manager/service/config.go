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

	"d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/manager/types"
)

func (s *rest) CreateConfig(ctx context.Context, json types.CreateConfigRequest) (*model.Config, error) {
	config := model.Config{
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

func (s *rest) DestroyConfig(ctx context.Context, id uint) error {
	config := model.Config{}
	if err := s.db.WithContext(ctx).First(&config, id).Error; err != nil {
		return err
	}

	if err := s.db.WithContext(ctx).Unscoped().Delete(&model.Config{}, id).Error; err != nil {
		return err
	}

	return nil
}

func (s *rest) UpdateConfig(ctx context.Context, id uint, json types.UpdateConfigRequest) (*model.Config, error) {
	config := model.Config{}
	if err := s.db.WithContext(ctx).First(&config, id).Updates(model.Config{
		Name:   json.Name,
		Value:  json.Value,
		BIO:    json.BIO,
		UserID: json.UserID,
	}).Error; err != nil {
		return nil, err
	}

	return &config, nil
}

func (s *rest) GetConfig(ctx context.Context, id uint) (*model.Config, error) {
	config := model.Config{}
	if err := s.db.WithContext(ctx).First(&config, id).Error; err != nil {
		return nil, err
	}

	return &config, nil
}

func (s *rest) GetConfigs(ctx context.Context, q types.GetConfigsQuery) (*[]model.Config, error) {
	configs := []model.Config{}
	if err := s.db.WithContext(ctx).Scopes(model.Paginate(q.Page, q.PerPage)).Where(&model.Config{
		Name:   q.Name,
		Value:  q.Value,
		UserID: q.UserID,
	}).Find(&configs).Error; err != nil {
		return nil, err
	}

	return &configs, nil
}

func (s *rest) ConfigTotalCount(ctx context.Context, q types.GetConfigsQuery) (int64, error) {
	var count int64
	if err := s.db.WithContext(ctx).Model(&model.Config{}).Where(&model.Config{
		Name:   q.Name,
		Value:  q.Value,
		UserID: q.UserID,
	}).Count(&count).Error; err != nil {
		return 0, err
	}

	return count, nil
}
