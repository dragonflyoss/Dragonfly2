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

func (s *rest) CreateCDNCluster(ctx context.Context, json types.CreateCDNClusterRequest) (*model.CDNCluster, error) {
	config, err := structutils.StructToMap(json.Config)
	if err != nil {
		return nil, err
	}

	cdnCluster := model.CDNCluster{
		Name:   json.Name,
		BIO:    json.BIO,
		Config: config,
	}

	if err := s.db.Create(&cdnCluster).Error; err != nil {
		return nil, err
	}

	return &cdnCluster, nil
}

func (s *rest) DestroyCDNCluster(ctx context.Context, id uint) error {
	cdnCluster := model.CDNCluster{}
	if err := s.db.First(&cdnCluster, id).Error; err != nil {
		return err
	}

	if err := s.db.Unscoped().Delete(&model.CDNCluster{}, id).Error; err != nil {
		return err
	}

	return nil
}

func (s *rest) CreateCDNClusterWithSecurityGroupDomain(ctx context.Context, json types.CreateCDNClusterRequest) (*model.CDNCluster, error) {
	securityGroup := model.SecurityGroup{
		Domain: json.SecurityGroupDomain,
	}
	if err := s.db.First(&securityGroup).Error; err != nil {
		return s.CreateCDNCluster(ctx, json)
	}

	config, err := structutils.StructToMap(json.Config)
	if err != nil {
		return nil, err
	}

	cdnCluster := model.CDNCluster{
		Name:   json.Name,
		BIO:    json.BIO,
		Config: config,
	}

	if err := s.db.Model(&securityGroup).Association("CDNClusters").Append(&cdnCluster); err != nil {
		return nil, err

	}

	return &cdnCluster, nil
}

func (s *rest) UpdateCDNCluster(ctx context.Context, id uint, json types.UpdateCDNClusterRequest) (*model.CDNCluster, error) {
	config, err := structutils.StructToMap(json.Config)
	if err != nil {
		return nil, err
	}

	cdnCluster := model.CDNCluster{}
	if err := s.db.First(&cdnCluster, id).Updates(model.CDNCluster{
		Name:   json.Name,
		BIO:    json.BIO,
		Config: config,
	}).Error; err != nil {
		return nil, err
	}

	return &cdnCluster, nil
}

func (s *rest) UpdateCDNClusterWithSecurityGroupDomain(ctx context.Context, id uint, json types.UpdateCDNClusterRequest) (*model.CDNCluster, error) {
	securityGroup := model.SecurityGroup{
		Domain: json.SecurityGroupDomain,
	}
	if err := s.db.First(&securityGroup).Error; err != nil {
		return s.UpdateCDNCluster(ctx, id, json)
	}

	config, err := structutils.StructToMap(json.Config)
	if err != nil {
		return nil, err
	}

	cdnCluster := model.CDNCluster{
		Name:   json.Name,
		BIO:    json.BIO,
		Config: config,
	}

	if err := s.db.Model(&securityGroup).Association("CDNClusters").Append(&cdnCluster); err != nil {
		return nil, err
	}

	return &cdnCluster, nil
}

func (s *rest) GetCDNCluster(ctx context.Context, id uint) (*model.CDNCluster, error) {
	cdnCluster := model.CDNCluster{}
	if err := s.db.First(&cdnCluster, id).Error; err != nil {
		return nil, err
	}

	return &cdnCluster, nil
}

func (s *rest) GetCDNClusters(ctx context.Context, q types.GetCDNClustersQuery) (*[]model.CDNCluster, error) {
	cdnClusters := []model.CDNCluster{}
	if err := s.db.Scopes(model.Paginate(q.Page, q.PerPage)).Where(&model.CDNCluster{
		Name: q.Name,
	}).Find(&cdnClusters).Error; err != nil {
		return nil, err
	}

	return &cdnClusters, nil
}

func (s *rest) CDNClusterTotalCount(ctx context.Context, q types.GetCDNClustersQuery) (int64, error) {
	var count int64
	if err := s.db.Model(&model.CDNCluster{}).Where(&model.CDNCluster{
		Name: q.Name,
	}).Count(&count).Error; err != nil {
		return 0, err
	}

	return count, nil
}

func (s *rest) AddCDNToCDNCluster(ctx context.Context, id, cdnID uint) error {
	cdnCluster := model.CDNCluster{}
	if err := s.db.First(&cdnCluster, id).Error; err != nil {
		return err
	}

	cdn := model.CDN{}
	if err := s.db.First(&cdn, cdnID).Error; err != nil {
		return err
	}

	if err := s.db.Model(&cdnCluster).Association("CDNs").Append(&cdn); err != nil {
		return err
	}

	return nil
}

func (s *rest) AddSchedulerClusterToCDNCluster(ctx context.Context, id, schedulerClusterID uint) error {
	cdnCluster := model.CDNCluster{}
	if err := s.db.First(&cdnCluster, id).Error; err != nil {
		return err
	}

	schedulerCluster := model.SchedulerCluster{}
	if err := s.db.First(&schedulerCluster, schedulerClusterID).Error; err != nil {
		return err
	}

	if err := s.db.Model(&cdnCluster).Association("SchedulerClusters").Append(&schedulerCluster); err != nil {
		return err
	}

	return nil
}
