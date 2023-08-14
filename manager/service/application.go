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
	"d7y.io/dragonfly/v2/pkg/structure"
)

func (s *service) CreateApplication(ctx context.Context, json types.CreateApplicationRequest) (*models.Application, error) {
	priority, err := structure.StructToMap(json.Priority)
	if err != nil {
		return nil, err
	}

	application := models.Application{
		Name:     json.Name,
		URL:      json.URL,
		BIO:      json.BIO,
		Priority: priority,
		UserID:   json.UserID,
	}

	if err := s.db.WithContext(ctx).Create(&application).Error; err != nil {
		return nil, err
	}

	return &application, nil
}

func (s *service) DestroyApplication(ctx context.Context, id uint) error {
	application := models.Application{}
	if err := s.db.WithContext(ctx).First(&application, id).Error; err != nil {
		return err
	}

	if err := s.db.WithContext(ctx).Unscoped().Delete(&models.Application{}, id).Error; err != nil {
		return err
	}

	return nil
}

func (s *service) UpdateApplication(ctx context.Context, id uint, json types.UpdateApplicationRequest) (*models.Application, error) {
	var (
		priority map[string]any
		err      error
	)
	if json.Priority != nil {
		priority, err = structure.StructToMap(json.Priority)
		if err != nil {
			return nil, err
		}
	}

	application := models.Application{}
	if err := s.db.WithContext(ctx).Preload("User").First(&application, id).Updates(models.Application{
		Name:     json.Name,
		URL:      json.URL,
		BIO:      json.BIO,
		Priority: priority,
		UserID:   json.UserID,
	}).Error; err != nil {
		return nil, err
	}

	return &application, nil
}

func (s *service) GetApplication(ctx context.Context, id uint) (*models.Application, error) {
	application := models.Application{}
	if err := s.db.WithContext(ctx).Preload("User").First(&application, id).Error; err != nil {
		return nil, err
	}

	return &application, nil
}

func (s *service) GetApplications(ctx context.Context, q types.GetApplicationsQuery) ([]models.Application, int64, error) {
	var count int64
	applications := []models.Application{}
	if err := s.db.WithContext(ctx).Scopes(models.Paginate(q.Page, q.PerPage)).Preload("User").Find(&applications).Limit(-1).Offset(-1).Count(&count).Error; err != nil {
		return nil, 0, err
	}

	return applications, count, nil
}
