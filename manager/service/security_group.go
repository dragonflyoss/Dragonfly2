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
)

func (s *service) CreateSecurityGroup(ctx context.Context, json types.CreateSecurityGroupRequest) (*models.SecurityGroup, error) {
	securityGroup := models.SecurityGroup{
		Name: json.Name,
		BIO:  json.BIO,
	}

	if err := s.db.WithContext(ctx).Create(&securityGroup).Error; err != nil {
		return nil, err
	}

	return &securityGroup, nil
}

func (s *service) DestroySecurityGroup(ctx context.Context, id uint) error {
	securityGroup := models.SecurityGroup{}
	if err := s.db.WithContext(ctx).First(&securityGroup, id).Error; err != nil {
		return err
	}

	if err := s.db.WithContext(ctx).Unscoped().Delete(&models.SecurityGroup{}, id).Error; err != nil {
		return err
	}

	return nil
}

func (s *service) UpdateSecurityGroup(ctx context.Context, id uint, json types.UpdateSecurityGroupRequest) (*models.SecurityGroup, error) {
	securityGroup := models.SecurityGroup{}
	if err := s.db.WithContext(ctx).First(&securityGroup, id).Updates(models.SecurityGroup{
		Name: json.Name,
		BIO:  json.BIO,
	}).Error; err != nil {
		return nil, err
	}

	return &securityGroup, nil
}

func (s *service) GetSecurityGroup(ctx context.Context, id uint) (*models.SecurityGroup, error) {
	securityGroup := models.SecurityGroup{}
	if err := s.db.WithContext(ctx).Preload("SecurityRules").First(&securityGroup, id).Error; err != nil {
		return nil, err
	}

	return &securityGroup, nil
}

func (s *service) GetSecurityGroups(ctx context.Context, q types.GetSecurityGroupsQuery) ([]models.SecurityGroup, int64, error) {
	var count int64
	var securityGroups []models.SecurityGroup
	if err := s.db.WithContext(ctx).Scopes(models.Paginate(q.Page, q.PerPage)).Where(&models.SecurityGroup{
		Name: q.Name,
	}).Preload("SecurityRules").Find(&securityGroups).Limit(-1).Offset(-1).Count(&count).Error; err != nil {
		return nil, 0, err
	}

	return securityGroups, count, nil
}

func (s *service) AddSchedulerClusterToSecurityGroup(ctx context.Context, id, schedulerClusterID uint) error {
	securityGroup := models.SecurityGroup{}
	if err := s.db.WithContext(ctx).First(&securityGroup, id).Error; err != nil {
		return err
	}

	schedulerCluster := models.SchedulerCluster{}
	if err := s.db.WithContext(ctx).First(&schedulerCluster, schedulerClusterID).Error; err != nil {
		return err
	}

	if err := s.db.WithContext(ctx).Model(&securityGroup).Association("SchedulerClusters").Append(&schedulerCluster); err != nil {
		return err
	}

	return nil
}

func (s *service) AddSeedPeerClusterToSecurityGroup(ctx context.Context, id, seedPeerClusterID uint) error {
	securityGroup := models.SecurityGroup{}
	if err := s.db.WithContext(ctx).First(&securityGroup, id).Error; err != nil {
		return err
	}

	seedPeerCluster := models.SeedPeerCluster{}
	if err := s.db.WithContext(ctx).First(&seedPeerCluster, seedPeerClusterID).Error; err != nil {
		return err
	}

	if err := s.db.WithContext(ctx).Model(&securityGroup).Association("SeedPeerClusters").Append(&seedPeerCluster); err != nil {
		return err
	}

	return nil
}

func (s *service) AddSecurityRuleToSecurityGroup(ctx context.Context, id, securityRuleID uint) error {
	securityGroup := models.SecurityGroup{}
	if err := s.db.WithContext(ctx).First(&securityGroup, id).Error; err != nil {
		return err
	}

	securityRule := models.SecurityRule{}
	if err := s.db.WithContext(ctx).First(&securityRule, securityRuleID).Error; err != nil {
		return err
	}

	if err := s.db.WithContext(ctx).Model(&securityGroup).Association("SecurityRules").Append(&securityRule); err != nil {
		return err
	}

	return nil
}

func (s *service) DestroySecurityRuleToSecurityGroup(ctx context.Context, id, securityRuleID uint) error {
	securityGroup := models.SecurityGroup{}
	if err := s.db.WithContext(ctx).First(&securityGroup, id).Error; err != nil {
		return err
	}

	securityRule := models.SecurityRule{}
	if err := s.db.WithContext(ctx).First(&securityRule, securityRuleID).Error; err != nil {
		return err
	}

	if err := s.db.WithContext(ctx).Model(&securityGroup).Association("SecurityRules").Delete(&securityRule); err != nil {
		return err
	}

	return nil
}
