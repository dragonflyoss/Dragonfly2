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

func (s *service) CreateApplication(ctx context.Context, json types.CreateApplicationRequest) (*model.Application, error) {
	application := model.Application{
		Name:              json.Name,
		DownloadRateLimit: json.DownloadRateLimit,
		URL:               json.URL,
		UserID:            json.UserID,
		BIO:               json.BIO,
		State:             json.State,
	}

	if err := s.db.WithContext(ctx).Preload("CDNClusters").Preload("SchedulerClusters").Preload("User").Create(&application).Error; err != nil {
		return nil, err
	}

	return &application, nil
}

func (s *service) DestroyApplication(ctx context.Context, id uint) error {
	application := model.Application{}
	if err := s.db.WithContext(ctx).First(&application, id).Error; err != nil {
		return err
	}

	if err := s.db.WithContext(ctx).Unscoped().Delete(&model.Application{}, id).Error; err != nil {
		return err
	}

	return nil
}

func (s *service) UpdateApplication(ctx context.Context, id uint, json types.UpdateApplicationRequest) (*model.Application, error) {
	application := model.Application{}
	if err := s.db.WithContext(ctx).Preload("CDNClusters").Preload("SchedulerClusters").Preload("User").First(&application, id).Updates(model.Application{
		Name:              json.Name,
		DownloadRateLimit: json.DownloadRateLimit,
		URL:               json.URL,
		State:             json.State,
		BIO:               json.BIO,
		UserID:            json.UserID,
	}).Error; err != nil {
		return nil, err
	}

	return &application, nil
}

func (s *service) GetApplication(ctx context.Context, id uint) (*model.Application, error) {
	application := model.Application{}
	if err := s.db.WithContext(ctx).Preload("CDNClusters").Preload("SchedulerClusters").Preload("User").First(&application, id).Error; err != nil {
		return nil, err
	}

	return &application, nil
}

func (s *service) GetApplications(ctx context.Context, q types.GetApplicationsQuery) (*[]model.Application, int64, error) {
	var count int64
	applications := []model.Application{}
	if err := s.db.WithContext(ctx).Scopes(model.Paginate(q.Page, q.PerPage)).Preload("CDNClusters").Preload("SchedulerClusters").Preload("User").Find(&applications).Count(&count).Error; err != nil {
		return nil, 0, err
	}

	return &applications, count, nil
}

func (s *service) AddSchedulerClusterToApplication(ctx context.Context, id, schedulerClusterID uint) error {
	application := model.Application{}
	if err := s.db.WithContext(ctx).First(&application, id).Error; err != nil {
		return err
	}

	schedulerCluster := model.SchedulerCluster{}
	if err := s.db.WithContext(ctx).First(&schedulerCluster, schedulerClusterID).Error; err != nil {
		return err
	}

	if err := s.db.WithContext(ctx).Model(&application).Association("SchedulerClusters").Append(&schedulerCluster); err != nil {
		return err
	}

	return nil
}

func (s *service) DeleteSchedulerClusterToApplication(ctx context.Context, id, schedulerClusterID uint) error {
	application := model.Application{}
	if err := s.db.WithContext(ctx).First(&application, id).Error; err != nil {
		return err
	}

	schedulerCluster := model.SchedulerCluster{}
	if err := s.db.WithContext(ctx).First(&schedulerCluster, schedulerClusterID).Error; err != nil {
		return err
	}

	if err := s.db.Model(&application).Association("SchedulerClusters").Delete(&schedulerCluster); err != nil {
		return err
	}

	return nil
}

func (s *service) AddCDNClusterToApplication(ctx context.Context, id, cdnClusterID uint) error {
	application := model.Application{}
	if err := s.db.WithContext(ctx).First(&application, id).Error; err != nil {
		return err
	}

	cdnCluster := model.CDNCluster{}
	if err := s.db.WithContext(ctx).First(&cdnCluster, cdnClusterID).Error; err != nil {
		return err
	}

	if err := s.db.WithContext(ctx).Model(&application).Association("CDNClusters").Append(&cdnCluster); err != nil {
		return err
	}

	return nil
}

func (s *service) DeleteCDNClusterToApplication(ctx context.Context, id, cdnClusterID uint) error {
	application := model.Application{}
	if err := s.db.WithContext(ctx).First(&application, id).Error; err != nil {
		return err
	}

	cdnCluster := model.CDNCluster{}
	if err := s.db.WithContext(ctx).First(&cdnCluster, cdnClusterID).Error; err != nil {
		return err
	}

	if err := s.db.Model(&application).Association("CDNClusters").Delete(&cdnCluster); err != nil {
		return err
	}

	return nil
}
