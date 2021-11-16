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

func (s *rest) CreateApplication(ctx context.Context, json types.CreateApplicationRequest) (*model.Application, error) {
	callsystem := model.Application{
		Name:              json.Name,
		DownloadRateLimit: json.DownloadRateLimit,
		URL:               json.URL,
		UserID:            json.UserID,
		BIO:               json.BIO,
		State:             json.State,
	}

	if err := s.db.WithContext(ctx).Create(&callsystem).Error; err != nil {
		return nil, err
	}

	return &callsystem, nil
}

func (s *rest) DestroyApplication(ctx context.Context, id uint) error {
	callsystem := model.Application{}
	if err := s.db.WithContext(ctx).First(&callsystem, id).Error; err != nil {
		return err
	}

	if err := s.db.WithContext(ctx).Unscoped().Delete(&model.Application{}, id).Error; err != nil {
		return err
	}

	return nil
}

func (s *rest) UpdateApplication(ctx context.Context, id uint, json types.UpdateApplicationRequest) (*model.Application, error) {
	schedulerCluster := model.Application{}
	if err := s.db.WithContext(ctx).First(&schedulerCluster, id).Updates(model.Application{
		Name:              json.Name,
		DownloadRateLimit: json.DownloadRateLimit,
		URL:               json.URL,
		State:             json.State,
		BIO:               json.BIO,
		UserID:            json.UserID,
	}).Error; err != nil {
		return nil, err
	}

	return &schedulerCluster, nil
}

func (s *rest) GetApplication(ctx context.Context, id uint) (*model.Application, error) {
	schedulerCluster := model.Application{}
	if err := s.db.WithContext(ctx).Preload("SchedulerClusters").First(&schedulerCluster, id).Error; err != nil {
		return nil, err
	}

	return &schedulerCluster, nil
}

func (s *rest) GetApplications(ctx context.Context, q types.GetApplicationsQuery) (*[]model.Application, int64, error) {
	var count int64
	applications := []model.Application{}
	if err := s.db.WithContext(ctx).Scopes(model.Paginate(q.Page, q.PerPage)).Preload("SchedulerClusters").Find(&applications).Count(&count).Error; err != nil {
		return nil, 0, err
	}

	return &applications, count, nil
}

func (s *rest) AddSchedulerClusterToApplication(ctx context.Context, id, schedulerClusterID uint) error {
	callsystem := model.Application{}
	if err := s.db.WithContext(ctx).First(&callsystem, id).Error; err != nil {
		return err
	}

	schedulerCluster := model.SchedulerCluster{}
	if err := s.db.WithContext(ctx).First(&schedulerCluster, schedulerClusterID).Error; err != nil {
		return err
	}

	if err := s.db.WithContext(ctx).Model(&callsystem).Association("SchedulerClusters").Append(&schedulerCluster); err != nil {
		return err
	}

	return nil
}

func (s *rest) DeleteSchedulerClusterToApplication(ctx context.Context, id, schedulerClusterID uint) error {
	callsystem := model.Application{}
	if err := s.db.WithContext(ctx).First(&callsystem, id).Error; err != nil {
		return err
	}

	schedulerCluster := model.SchedulerCluster{}
	if err := s.db.WithContext(ctx).First(&schedulerCluster, schedulerClusterID).Error; err != nil {
		return err
	}

	if err := s.db.Model(&callsystem).Association("SchedulerClusters").Delete(&schedulerCluster); err != nil {
		return err
	}

	return nil
}

func (s *rest) AddCDNClusterToApplication(ctx context.Context, id, cdnClusterID uint) error {
	callsystem := model.Application{}
	if err := s.db.WithContext(ctx).First(&callsystem, id).Error; err != nil {
		return err
	}

	cdnCluster := model.CDNCluster{}
	if err := s.db.WithContext(ctx).First(&cdnCluster, cdnClusterID).Error; err != nil {
		return err
	}

	if err := s.db.WithContext(ctx).Model(&callsystem).Association("CDNClusters").Append(&cdnCluster); err != nil {
		return err
	}

	return nil
}

func (s *rest) DeleteCDNClusterToApplication(ctx context.Context, id, cdnClusterID uint) error {
	callsystem := model.Application{}
	if err := s.db.WithContext(ctx).First(&callsystem, id).Error; err != nil {
		return err
	}

	cdnCluster := model.CDNCluster{}
	if err := s.db.WithContext(ctx).First(&cdnCluster, cdnClusterID).Error; err != nil {
		return err
	}

	if err := s.db.Model(&callsystem).Association("CDNClusters").Delete(&cdnCluster); err != nil {
		return err
	}

	return nil
}
