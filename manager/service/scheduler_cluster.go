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
	"d7y.io/dragonfly/v2/pkg/structure"
)

func (s *service) CreateSchedulerCluster(ctx context.Context, json types.CreateSchedulerClusterRequest) (*model.SchedulerCluster, error) {
	config, err := structure.StructToMap(json.Config)
	if err != nil {
		return nil, err
	}

	clientConfig, err := structure.StructToMap(json.ClientConfig)
	if err != nil {
		return nil, err
	}

	scopes, err := structure.StructToMap(json.Scopes)
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

	if json.SeedPeerClusterID > 0 {
		if err := s.AddSchedulerClusterToSeedPeerCluster(ctx, json.SeedPeerClusterID, schedulerCluster.ID); err != nil {
			return nil, err
		}
	}

	if json.SecurityGroupID > 0 {
		if err := s.AddSchedulerClusterToSecurityGroup(ctx, json.SecurityGroupID, schedulerCluster.ID); err != nil {
			return nil, err
		}
	}

	return &schedulerCluster, nil
}

func (s *service) DestroySchedulerCluster(ctx context.Context, id uint) error {
	schedulerCluster := model.SchedulerCluster{}
	if err := s.db.WithContext(ctx).Preload("Schedulers").First(&schedulerCluster, id).Error; err != nil {
		return err
	}

	if len(schedulerCluster.Schedulers) != 0 {
		return errors.New("scheduler cluster exists scheduler")
	}

	if err := s.db.WithContext(ctx).Delete(&model.SchedulerCluster{}, id).Error; err != nil {
		return err
	}

	return nil
}

func (s *service) UpdateSchedulerCluster(ctx context.Context, id uint, json types.UpdateSchedulerClusterRequest) (*model.SchedulerCluster, error) {
	config, err := structure.StructToMap(json.Config)
	if err != nil {
		return nil, err
	}

	clientConfig, err := structure.StructToMap(json.ClientConfig)
	if err != nil {
		return nil, err
	}

	scopes, err := structure.StructToMap(json.Scopes)
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

	if json.SeedPeerClusterID > 0 {
		if err := s.AddSchedulerClusterToSeedPeerCluster(ctx, json.SeedPeerClusterID, schedulerCluster.ID); err != nil {
			return nil, err
		}
	}

	if json.SecurityGroupID > 0 {
		if err := s.AddSchedulerClusterToSecurityGroup(ctx, json.SecurityGroupID, schedulerCluster.ID); err != nil {
			return nil, err
		}
	}

	return &schedulerCluster, nil
}

func (s *service) GetSchedulerCluster(ctx context.Context, id uint) (*model.SchedulerCluster, error) {
	schedulerCluster := model.SchedulerCluster{}
	if err := s.db.WithContext(ctx).Preload("SeedPeerClusters").Preload("SecurityGroup").First(&schedulerCluster, id).Error; err != nil {
		return nil, err
	}

	return &schedulerCluster, nil
}

func (s *service) GetSchedulerClusters(ctx context.Context, q types.GetSchedulerClustersQuery) ([]model.SchedulerCluster, int64, error) {
	var count int64
	var schedulerClusters []model.SchedulerCluster
	if err := s.db.WithContext(ctx).Scopes(model.Paginate(q.Page, q.PerPage)).Where(&model.SchedulerCluster{
		Name: q.Name,
	}).Preload("SeedPeerClusters").Preload("SecurityGroup").Find(&schedulerClusters).Limit(-1).Offset(-1).Count(&count).Error; err != nil {
		return nil, 0, err
	}

	return schedulerClusters, count, nil
}

func (s *service) AddSchedulerToSchedulerCluster(ctx context.Context, id, schedulerID uint) error {
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
