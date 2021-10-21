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
	"d7y.io/dragonfly/v2/pkg/util/structutils"
)

func (s *rest) CreateSchedulerCluster(ctx context.Context, json types.CreateSchedulerClusterRequest) (*model.SchedulerCluster, error) {
	config, err := structutils.StructToMap(json.Config)
	if err != nil {
		return nil, err
	}

	clientConfig, err := structutils.StructToMap(json.ClientConfig)
	if err != nil {
		return nil, err
	}

	scopes, err := structutils.StructToMap(json.Scopes)
	if err != nil {
		return nil, err
	}

	schedulerCluster := model.SchedulerCluster{
		Name:         json.Name,
		BIO:          json.BIO,
		Config:       config,
		ClientConfig: clientConfig,
		Scopes:       scopes,
		IsDefault:    json.IsDefault,
	}

	if err := s.db.WithContext(ctx).Create(&schedulerCluster).Error; err != nil {
		return nil, err
	}

	if json.CDNClusterID > 0 {
		if err := s.AddSchedulerClusterToCDNCluster(ctx, json.CDNClusterID, schedulerCluster.ID); err != nil {
			return nil, err
		}
	}

	return &schedulerCluster, nil
}

func (s *rest) CreateSchedulerClusterWithSecurityGroupDomain(ctx context.Context, json types.CreateSchedulerClusterRequest) (*model.SchedulerCluster, error) {
	securityGroup := model.SecurityGroup{
		Domain: json.SecurityGroupDomain,
	}
	if err := s.db.WithContext(ctx).First(&securityGroup).Error; err != nil {
		return s.CreateSchedulerCluster(ctx, json)
	}

	config, err := structutils.StructToMap(json.Config)
	if err != nil {
		return nil, err
	}

	clientConfig, err := structutils.StructToMap(json.ClientConfig)
	if err != nil {
		return nil, err
	}

	scopes, err := structutils.StructToMap(json.Scopes)
	if err != nil {
		return nil, err
	}

	schedulerCluster := model.SchedulerCluster{
		Name:         json.Name,
		BIO:          json.BIO,
		Config:       config,
		ClientConfig: clientConfig,
		Scopes:       scopes,
		IsDefault:    json.IsDefault,
	}

	if err := s.db.WithContext(ctx).Model(&securityGroup).Association("SchedulerClusters").Append(&schedulerCluster); err != nil {
		return nil, err
	}

	if json.CDNClusterID > 0 {
		if err := s.AddSchedulerClusterToCDNCluster(ctx, json.CDNClusterID, schedulerCluster.ID); err != nil {
			return nil, err
		}
	}

	return &schedulerCluster, nil
}

func (s *rest) DestroySchedulerCluster(ctx context.Context, id uint) error {
	schedulerCluster := model.SchedulerCluster{}
	if err := s.db.WithContext(ctx).First(&schedulerCluster, id).Error; err != nil {
		return err
	}

	if err := s.db.WithContext(ctx).Unscoped().Delete(&model.SchedulerCluster{}, id).Error; err != nil {
		return err
	}

	return nil
}

func (s *rest) UpdateSchedulerCluster(ctx context.Context, id uint, json types.UpdateSchedulerClusterRequest) (*model.SchedulerCluster, error) {
	config, err := structutils.StructToMap(json.Config)
	if err != nil {
		return nil, err
	}

	clientConfig, err := structutils.StructToMap(json.ClientConfig)
	if err != nil {
		return nil, err
	}

	scopes, err := structutils.StructToMap(json.Scopes)
	if err != nil {
		return nil, err
	}

	schedulerCluster := model.SchedulerCluster{}
	if err := s.db.WithContext(ctx).First(&schedulerCluster, id).Updates(model.SchedulerCluster{
		Name:         json.Name,
		BIO:          json.BIO,
		Config:       config,
		ClientConfig: clientConfig,
		Scopes:       scopes,
		IsDefault:    json.IsDefault,
	}).Error; err != nil {
		return nil, err
	}

	if json.CDNClusterID > 0 {
		if err := s.AddSchedulerClusterToCDNCluster(ctx, json.CDNClusterID, schedulerCluster.ID); err != nil {
			return nil, err
		}
	}

	return &schedulerCluster, nil
}

func (s *rest) UpdateSchedulerClusterWithSecurityGroupDomain(ctx context.Context, id uint, json types.UpdateSchedulerClusterRequest) (*model.SchedulerCluster, error) {
	securityGroup := model.SecurityGroup{
		Domain: json.SecurityGroupDomain,
	}
	if err := s.db.WithContext(ctx).First(&securityGroup).Error; err != nil {
		return s.UpdateSchedulerCluster(ctx, id, json)
	}

	config, err := structutils.StructToMap(json.Config)
	if err != nil {
		return nil, err
	}

	clientConfig, err := structutils.StructToMap(json.ClientConfig)
	if err != nil {
		return nil, err
	}

	scopes, err := structutils.StructToMap(json.Scopes)
	if err != nil {
		return nil, err
	}

	schedulerCluster := model.SchedulerCluster{
		Name:         json.Name,
		BIO:          json.BIO,
		Config:       config,
		ClientConfig: clientConfig,
		Scopes:       scopes,
		IsDefault:    json.IsDefault,
	}

	if err := s.db.WithContext(ctx).Model(&securityGroup).Association("SchedulerClusters").Append(&schedulerCluster); err != nil {
		return nil, err
	}

	if json.CDNClusterID > 0 {
		if err := s.AddSchedulerClusterToCDNCluster(ctx, json.CDNClusterID, schedulerCluster.ID); err != nil {
			return nil, err
		}
	}

	return &schedulerCluster, nil
}

func (s *rest) GetSchedulerCluster(ctx context.Context, id uint) (*model.SchedulerCluster, error) {
	schedulerCluster := model.SchedulerCluster{}
	if err := s.db.WithContext(ctx).Preload("CDNClusters").First(&schedulerCluster, id).Error; err != nil {
		return nil, err
	}

	return &schedulerCluster, nil
}

func (s *rest) GetSchedulerClusters(ctx context.Context, q types.GetSchedulerClustersQuery) (*[]model.SchedulerCluster, error) {
	schedulerClusters := []model.SchedulerCluster{}
	if err := s.db.WithContext(ctx).Scopes(model.Paginate(q.Page, q.PerPage)).Where(&model.SchedulerCluster{
		Name: q.Name,
	}).Preload("CDNClusters").Find(&schedulerClusters).Error; err != nil {
		return nil, err
	}

	return &schedulerClusters, nil
}

func (s *rest) SchedulerClusterTotalCount(ctx context.Context, q types.GetSchedulerClustersQuery) (int64, error) {
	var count int64
	if err := s.db.WithContext(ctx).Model(&model.SchedulerCluster{}).Where(&model.SchedulerCluster{
		Name: q.Name,
	}).Count(&count).Error; err != nil {
		return 0, err
	}

	return count, nil
}

func (s *rest) AddSchedulerToSchedulerCluster(ctx context.Context, id, schedulerID uint) error {
	schedulerCluster := model.SchedulerCluster{}
	if err := s.db.WithContext(ctx).First(&schedulerCluster, id).Error; err != nil {
		return err
	}

	scheduler := model.Scheduler{}
	if err := s.db.WithContext(ctx).First(&scheduler, schedulerID).Error; err != nil {
		return err
	}

	if err := s.db.WithContext(ctx).Model(&schedulerCluster).Association("Schedulers").Append(&scheduler); err != nil {
		return err
	}

	return nil
}
