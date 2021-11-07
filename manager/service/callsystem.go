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
	"d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/manager/types"
)

func (s *rest) CreateCallSystem(json types.CreateCallSystemRequest) (*model.CallSystem, error) {
	callsystem := model.CallSystem{
		Name:           json.Name,
		LimitFrequency: json.LimitFrequency,
		URLRegexs:      json.URLRegexs,
		ValidityPeriod: json.ValidityPeriod,
		IsEnable:       json.IsEnable,
	}

	if err := s.db.Create(&callsystem).Error; err != nil {
		return nil, err
	}

	return &callsystem, nil
}

func (s *rest) DestroyCallSystem(id uint) error {
	callsystem := model.CallSystem{}
	if err := s.db.First(&callsystem, id).Error; err != nil {
		return err
	}

	if err := s.db.Unscoped().Delete(&model.CallSystem{}, id).Error; err != nil {
		return err
	}

	return nil
}

func (s *rest) UpdateCallSystem(id uint, json types.UpdateCallSystemRequest) (*model.CallSystem, error) {
	schedulerCluster := model.CallSystem{}
	if err := s.db.First(&schedulerCluster, id).Updates(model.CallSystem{
		Name:           json.Name,
		LimitFrequency: json.LimitFrequency,
		URLRegexs:      json.URLRegexs,
		ValidityPeriod: json.ValidityPeriod,
		IsEnable:       json.IsEnable,
	}).Error; err != nil {
		return nil, err
	}

	return &schedulerCluster, nil
}

func (s *rest) GetCallSystem(id uint) (*model.CallSystem, error) {
	schedulerCluster := model.CallSystem{}
	if err := s.db.Preload("CDNClusters").First(&schedulerCluster, id).Error; err != nil {
		return nil, err
	}

	return &schedulerCluster, nil
}

func (s *rest) GetCallSystems(q types.GetCallSystemsQuery) (*[]model.CallSystem, error) {
	schedulerClusters := []model.CallSystem{}
	if err := s.db.Scopes(model.Paginate(q.Page, q.PerPage)).Where(&model.CallSystem{
		Name: q.Name,
	}).Preload("CDNClusters").Find(&schedulerClusters).Error; err != nil {
		return nil, err
	}

	return &schedulerClusters, nil
}

func (s *rest) CallSystemTotalCount(q types.GetCallSystemsQuery) (int64, error) {
	var count int64
	if err := s.db.Model(&model.CallSystem{}).Where(&model.CallSystem{
		Name: q.Name,
	}).Count(&count).Error; err != nil {
		return 0, err
	}

	return count, nil
}

func (s *rest) AddSchedulerClusterToCallSystem(id, schedulerClusterID uint) error {
	callsystem := model.CallSystem{}
	if err := s.db.First(&callsystem, id).Error; err != nil {
		return err
	}

	schedulerCluster := model.SchedulerCluster{}
	if err := s.db.First(&schedulerCluster, schedulerClusterID).Error; err != nil {
		return err
	}

	if err := s.db.Model(&callsystem).Association("SchedulerClusters").Append(&schedulerCluster); err != nil {
		return err
	}

	return nil
}

func (s *rest) DeleteSchedulerClusterToCallSystem(id, schedulerClusterID uint) error {
	callsystem := model.CallSystem{}
	if err := s.db.First(&callsystem, id).Error; err != nil {
		return err
	}

	schedulerCluster := model.SchedulerCluster{}
	if err := s.db.First(&schedulerCluster, schedulerClusterID).Error; err != nil {
		return err
	}

	if err := s.db.Model(&callsystem).Association("SchedulerClusters").Delete(&schedulerCluster); err != nil {
		return err
	}

	return nil
}
