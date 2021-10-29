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

func (s *rest) CreateSecurityGroup(ctx context.Context, json types.CreateSecurityGroupRequest) (*model.SecurityGroup, error) {
	securityGroup := model.SecurityGroup{
		Name:        json.Name,
		BIO:         json.BIO,
		Domain:      json.Domain,
		ProxyDomain: json.ProxyDomain,
	}

	if err := s.db.WithContext(ctx).Create(&securityGroup).Error; err != nil {
		return nil, err
	}

	return &securityGroup, nil
}

func (s *rest) DestroySecurityGroup(ctx context.Context, id uint) error {
	securityGroup := model.SecurityGroup{}
	if err := s.db.WithContext(ctx).First(&securityGroup, id).Error; err != nil {
		return err
	}

	if err := s.db.WithContext(ctx).Unscoped().Delete(&model.SecurityGroup{}, id).Error; err != nil {
		return err
	}

	return nil
}

func (s *rest) UpdateSecurityGroup(ctx context.Context, id uint, json types.UpdateSecurityGroupRequest) (*model.SecurityGroup, error) {
	securityGroup := model.SecurityGroup{}
	if err := s.db.WithContext(ctx).First(&securityGroup, id).Updates(model.SecurityGroup{
		Name:        json.Name,
		BIO:         json.BIO,
		Domain:      json.Domain,
		ProxyDomain: json.ProxyDomain,
	}).Error; err != nil {
		return nil, err
	}

	return &securityGroup, nil
}

func (s *rest) GetSecurityGroup(ctx context.Context, id uint) (*model.SecurityGroup, error) {
	securityGroup := model.SecurityGroup{}
	if err := s.db.WithContext(ctx).First(&securityGroup, id).Error; err != nil {
		return nil, err
	}

	return &securityGroup, nil
}

func (s *rest) GetSecurityGroups(ctx context.Context, q types.GetSecurityGroupsQuery) (*[]model.SecurityGroup, int64, error) {
	var count int64
	var securityGroups []model.SecurityGroup
	if err := s.db.WithContext(ctx).Scopes(model.Paginate(q.Page, q.PerPage)).Where(&model.SecurityGroup{
		Name:   q.Name,
		Domain: q.Domain,
	}).Find(&securityGroups).Count(&count).Error; err != nil {
		return nil, 0, err
	}

	return &securityGroups, count, nil
}

func (s *rest) AddSchedulerClusterToSecurityGroup(ctx context.Context, id, schedulerClusterID uint) error {
	securityGroup := model.SecurityGroup{}
	if err := s.db.WithContext(ctx).First(&securityGroup, id).Error; err != nil {
		return err
	}

	schedulerCluster := model.SchedulerCluster{}
	if err := s.db.WithContext(ctx).First(&schedulerCluster, schedulerClusterID).Error; err != nil {
		return err
	}

	if err := s.db.WithContext(ctx).Model(&securityGroup).Association("SchedulerClusters").Append(&schedulerCluster); err != nil {
		return err
	}

	return nil
}

func (s *rest) AddCDNClusterToSecurityGroup(ctx context.Context, id, cdnClusterID uint) error {
	securityGroup := model.SecurityGroup{}
	if err := s.db.WithContext(ctx).First(&securityGroup, id).Error; err != nil {
		return err
	}

	cdnCluster := model.CDNCluster{}
	if err := s.db.WithContext(ctx).First(&cdnCluster, cdnClusterID).Error; err != nil {
		return err
	}

	if err := s.db.WithContext(ctx).Model(&securityGroup).Association("CDNClusters").Append(&cdnCluster); err != nil {
		return err
	}

	return nil
}
