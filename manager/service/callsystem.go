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

func (s *rest) CreateCallSystem(ctx context.Context, json types.CreateCallSystemRequest) (*model.CallSystem, error) {
	callsystem := model.CallSystem{
		Name:           json.Name,
		LimitFrequency: json.LimitFrequency,
		URLRegex:       json.URLRegex,
	}

	if err := s.db.WithContext(ctx).Create(&callsystem).Error; err != nil {
		return nil, err
	}

	return &callsystem, nil
}

func (s *rest) DestroyCallSystem(ctx context.Context, id uint) error {
	callsystem := model.CallSystem{}
	if err := s.db.WithContext(ctx).First(&callsystem, id).Error; err != nil {
		return err
	}

	if err := s.db.WithContext(ctx).Unscoped().Delete(&model.CallSystem{}, id).Error; err != nil {
		return err
	}

	return nil
}

func (s *rest) UpdateCallSystem(ctx context.Context, id uint, json types.UpdateCallSystemRequest) (*model.CallSystem, error) {
	schedulerCluster := model.CallSystem{}
	if err := s.db.WithContext(ctx).First(&schedulerCluster, id).Updates(model.CallSystem{
		Name:           json.Name,
		LimitFrequency: json.LimitFrequency,
		URLRegex:       json.URLRegex,
		State:          json.State,
	}).Error; err != nil {
		return nil, err
	}

	return &schedulerCluster, nil
}

func (s *rest) GetCallSystem(ctx context.Context, id uint) (*model.CallSystem, error) {
	schedulerCluster := model.CallSystem{}
	if err := s.db.WithContext(ctx).Preload("SchedulerClusters").First(&schedulerCluster, id).Error; err != nil {
		return nil, err
	}

	return &schedulerCluster, nil
}

func (s *rest) GetCallSystems(ctx context.Context, q types.GetCallSystemsQuery) (*[]model.CallSystem, int64, error) {
	var count int64
	callSystems := []model.CallSystem{}
	if err := s.db.WithContext(ctx).Scopes(model.Paginate(q.Page, q.PerPage)).Preload("SchedulerClusters").Find(&callSystems).Count(&count).Error; err != nil {
		return nil, 0, err
	}

	return &callSystems, count, nil
}

func (s *rest) AddSchedulerClusterToCallSystem(ctx context.Context, id, schedulerClusterID uint) error {
	callsystem := model.CallSystem{}
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

func (s *rest) DeleteSchedulerClusterToCallSystem(ctx context.Context, id, schedulerClusterID uint) error {
	callsystem := model.CallSystem{}
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

func (s *rest) AddCDNClusterToCallSystem(ctx context.Context, id, cdnClusterID uint) error {
	callsystem := model.CallSystem{}
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

func (s *rest) DeleteCDNClusterToCallSystem(ctx context.Context, id, cdnClusterID uint) error {
	callsystem := model.CallSystem{}
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
