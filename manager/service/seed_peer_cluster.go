/*
 *     Copyright 2022 The Dragonfly Authors
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

func (s *service) CreateSeedPeerCluster(ctx context.Context, json types.CreateSeedPeerClusterRequest) (*model.SeedPeerCluster, error) {
	config, err := structure.StructToMap(json.Config)
	if err != nil {
		return nil, err
	}

	scopes, err := structure.StructToMap(json.Scopes)
	if err != nil {
		return nil, err
	}

	seedPeerCluster := model.SeedPeerCluster{
		Name:      json.Name,
		BIO:       json.BIO,
		Config:    config,
		Scopes:    scopes,
		IsDefault: json.IsDefault,
	}

	if err := s.db.WithContext(ctx).Create(&seedPeerCluster).Error; err != nil {
		return nil, err
	}

	return &seedPeerCluster, nil
}

func (s *service) DestroySeedPeerCluster(ctx context.Context, id uint) error {
	seedPeerCluster := model.SeedPeerCluster{}
	if err := s.db.WithContext(ctx).Preload("SeedPeers").First(&seedPeerCluster, id).Error; err != nil {
		return err
	}

	if len(seedPeerCluster.SeedPeers) != 0 {
		return errors.New("seedPeer cluster exists seedPeer")
	}

	if err := s.db.WithContext(ctx).Delete(&model.SeedPeerCluster{}, id).Error; err != nil {
		return err
	}

	return nil
}

func (s *service) UpdateSeedPeerCluster(ctx context.Context, id uint, json types.UpdateSeedPeerClusterRequest) (*model.SeedPeerCluster, error) {
	config, err := structure.StructToMap(json.Config)
	if err != nil {
		return nil, err
	}

	scopes, err := structure.StructToMap(json.Scopes)
	if err != nil {
		return nil, err
	}

	seedPeerCluster := model.SeedPeerCluster{}
	if err := s.db.WithContext(ctx).First(&seedPeerCluster, id).Updates(model.SeedPeerCluster{
		Name:      json.Name,
		BIO:       json.BIO,
		Config:    config,
		Scopes:    scopes,
		IsDefault: json.IsDefault,
	}).Error; err != nil {
		return nil, err
	}

	return &seedPeerCluster, nil
}

func (s *service) GetSeedPeerCluster(ctx context.Context, id uint) (*model.SeedPeerCluster, error) {
	seedPeerCluster := model.SeedPeerCluster{}
	if err := s.db.WithContext(ctx).First(&seedPeerCluster, id).Error; err != nil {
		return nil, err
	}

	return &seedPeerCluster, nil
}

func (s *service) GetSeedPeerClusters(ctx context.Context, q types.GetSeedPeerClustersQuery) ([]model.SeedPeerCluster, int64, error) {
	var count int64
	var seedPeerClusters []model.SeedPeerCluster
	if err := s.db.WithContext(ctx).Scopes(model.Paginate(q.Page, q.PerPage)).Where(&model.SeedPeerCluster{
		Name: q.Name,
	}).Find(&seedPeerClusters).Limit(-1).Offset(-1).Count(&count).Error; err != nil {
		return nil, 0, err
	}

	return seedPeerClusters, count, nil
}

func (s *service) AddSeedPeerToSeedPeerCluster(ctx context.Context, id, seedPeerID uint) error {
	seedPeerCluster := model.SeedPeerCluster{}
	if err := s.db.WithContext(ctx).First(&seedPeerCluster, id).Error; err != nil {
		return err
	}

	seedPeer := model.SeedPeer{}
	if err := s.db.WithContext(ctx).First(&seedPeer, seedPeerID).Error; err != nil {
		return err
	}

	if err := s.db.WithContext(ctx).Model(&seedPeerCluster).Association("SeedPeers").Append(&seedPeer); err != nil {
		return err
	}

	return nil
}

func (s *service) AddSchedulerClusterToSeedPeerCluster(ctx context.Context, id, schedulerClusterID uint) error {
	seedPeerCluster := model.SeedPeerCluster{}
	if err := s.db.WithContext(ctx).First(&seedPeerCluster, id).Error; err != nil {
		return err
	}

	schedulerCluster := model.SchedulerCluster{}
	if err := s.db.WithContext(ctx).First(&schedulerCluster, schedulerClusterID).Error; err != nil {
		return err
	}

	seedPeerClusters := []model.SeedPeerCluster{}
	if err := s.db.WithContext(ctx).Model(&schedulerCluster).Association("SeedPeerClusters").Find(&seedPeerClusters); err != nil {
		return err
	}

	if err := s.db.WithContext(ctx).Model(&schedulerCluster).Association("SeedPeerClusters").Delete(seedPeerClusters); err != nil {
		return err
	}

	if err := s.db.WithContext(ctx).Model(&seedPeerCluster).Association("SchedulerClusters").Append(&schedulerCluster); err != nil {
		return err
	}

	return nil
}
