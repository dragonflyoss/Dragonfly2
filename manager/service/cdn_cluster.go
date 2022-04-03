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
	"errors"

	"d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/manager/types"
	"d7y.io/dragonfly/v2/pkg/util/structutils"
)

func (s *service) CreateCDNCluster(ctx context.Context, json types.CreateCDNClusterRequest) (*model.CDNCluster, error) {
	config, err := structutils.StructToMap(json.Config)
	if err != nil {
		return nil, err
	}

	cdnCluster := model.CDNCluster{
		Name:      json.Name,
		BIO:       json.BIO,
		Config:    config,
		IsDefault: json.IsDefault,
	}

	if err := s.db.WithContext(ctx).Create(&cdnCluster).Error; err != nil {
		return nil, err
	}

	return &cdnCluster, nil
}

func (s *service) DestroyCDNCluster(ctx context.Context, id uint) error {
	cdnCluster := model.CDNCluster{}
	if err := s.db.WithContext(ctx).Preload("CDNs").First(&cdnCluster, id).Error; err != nil {
		return err
	}

	if len(cdnCluster.CDNs) != 0 {
		return errors.New("cdn cluster exists cdn")
	}

	if err := s.db.WithContext(ctx).Unscoped().Delete(&model.CDNCluster{}, id).Error; err != nil {
		return err
	}

	return nil
}

func (s *service) UpdateCDNCluster(ctx context.Context, id uint, json types.UpdateCDNClusterRequest) (*model.CDNCluster, error) {
	config, err := structutils.StructToMap(json.Config)
	if err != nil {
		return nil, err
	}

	cdnCluster := model.CDNCluster{}
	if err := s.db.WithContext(ctx).First(&cdnCluster, id).Updates(model.CDNCluster{
		Name:      json.Name,
		BIO:       json.BIO,
		Config:    config,
		IsDefault: json.IsDefault,
	}).Error; err != nil {
		return nil, err
	}

	return &cdnCluster, nil
}

func (s *service) GetCDNCluster(ctx context.Context, id uint) (*model.CDNCluster, error) {
	cdnCluster := model.CDNCluster{}
	if err := s.db.WithContext(ctx).First(&cdnCluster, id).Error; err != nil {
		return nil, err
	}

	return &cdnCluster, nil
}

func (s *service) GetCDNClusters(ctx context.Context, q types.GetCDNClustersQuery) (*[]model.CDNCluster, int64, error) {
	var count int64
	var cdnClusters []model.CDNCluster
	if err := s.db.WithContext(ctx).Scopes(model.Paginate(q.Page, q.PerPage)).Where(&model.CDNCluster{
		Name: q.Name,
	}).Find(&cdnClusters).Count(&count).Error; err != nil {
		return nil, 0, err
	}

	return &cdnClusters, count, nil
}

func (s *service) AddCDNToCDNCluster(ctx context.Context, id, cdnID uint) error {
	cdnCluster := model.CDNCluster{}
	if err := s.db.WithContext(ctx).First(&cdnCluster, id).Error; err != nil {
		return err
	}

	cdn := model.CDN{}
	if err := s.db.WithContext(ctx).First(&cdn, cdnID).Error; err != nil {
		return err
	}

	if err := s.db.WithContext(ctx).Model(&cdnCluster).Association("CDNs").Append(&cdn); err != nil {
		return err
	}

	return nil
}

func (s *service) AddSchedulerClusterToCDNCluster(ctx context.Context, id, schedulerClusterID uint) error {
	cdnCluster := model.CDNCluster{}
	if err := s.db.WithContext(ctx).First(&cdnCluster, id).Error; err != nil {
		return err
	}

	schedulerCluster := model.SchedulerCluster{}
	if err := s.db.WithContext(ctx).First(&schedulerCluster, schedulerClusterID).Error; err != nil {
		return err
	}

	cdnClusters := []model.CDNCluster{}
	if err := s.db.WithContext(ctx).Model(&schedulerCluster).Association("CDNClusters").Find(&cdnClusters); err != nil {
		return err
	}

	if err := s.db.WithContext(ctx).Model(&schedulerCluster).Association("CDNClusters").Delete(cdnClusters); err != nil {
		return err
	}

	if err := s.db.WithContext(ctx).Model(&cdnCluster).Association("SchedulerClusters").Append(&schedulerCluster); err != nil {
		return err
	}

	return nil
}
