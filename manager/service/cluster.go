/*
 *     Copyright 2023 The Dragonfly Authors
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

	"d7y.io/dragonfly/v2/manager/models"
	"d7y.io/dragonfly/v2/manager/types"
	"d7y.io/dragonfly/v2/pkg/structure"
)

func (s *service) CreateCluster(ctx context.Context, json types.CreateClusterRequest) (*types.CreateClusterResponse, error) {
	schedulerClusterConfig, err := structure.StructToMap(json.SchedulerClusterConfig)
	if err != nil {
		return nil, err
	}

	seedPeerClusterConfig, err := structure.StructToMap(json.SeedPeerClusterConfig)
	if err != nil {
		return nil, err
	}

	peerClusterConfig, err := structure.StructToMap(json.PeerClusterConfig)
	if err != nil {
		return nil, err
	}

	scopes, err := structure.StructToMap(json.Scopes)
	if err != nil {
		return nil, err
	}

	// Create cluster with transaction.
	tx := s.db.WithContext(ctx).Begin()
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()

	if err := tx.Error; err != nil {
		return nil, err
	}

	schedulerCluster := models.SchedulerCluster{
		Name:         json.Name,
		BIO:          json.BIO,
		Config:       schedulerClusterConfig,
		ClientConfig: peerClusterConfig,
		Scopes:       scopes,
		IsDefault:    json.IsDefault,
	}

	if err := tx.WithContext(ctx).Create(&schedulerCluster).Error; err != nil {
		tx.Rollback()
		return nil, err
	}

	seedPeerCluster := models.SeedPeerCluster{
		Name:   json.Name,
		BIO:    json.BIO,
		Config: seedPeerClusterConfig,
	}

	if err := tx.WithContext(ctx).Create(&seedPeerCluster).Error; err != nil {
		tx.Rollback()
		return nil, err
	}

	if err := tx.WithContext(ctx).Model(&seedPeerCluster).Association("SchedulerClusters").Append(&schedulerCluster); err != nil {
		tx.Rollback()
		return nil, err
	}

	if tx.Commit().Error != nil {
		return nil, err
	}

	return &types.CreateClusterResponse{
		ID:                     schedulerCluster.ID,
		Name:                   schedulerCluster.Name,
		BIO:                    schedulerCluster.BIO,
		Scopes:                 json.Scopes,
		SchedulerClusterID:     schedulerCluster.ID,
		SeedPeerClusterID:      seedPeerCluster.ID,
		SchedulerClusterConfig: json.SchedulerClusterConfig,
		SeedPeerClusterConfig:  json.SeedPeerClusterConfig,
		PeerClusterConfig:      json.PeerClusterConfig,
		CreatedAt:              schedulerCluster.CreatedAt,
		UpdatedAt:              schedulerCluster.UpdatedAt,
		IsDefault:              schedulerCluster.IsDefault,
	}, nil
}

func (s *service) DestroyCluster(ctx context.Context, id uint) error {
	schedulerCluster := models.SchedulerCluster{}
	if err := s.db.WithContext(ctx).Preload("Schedulers").First(&schedulerCluster, id).Error; err != nil {
		return err
	}

	if len(schedulerCluster.Schedulers) != 0 {
		return errors.New("scheduler cluster exists scheduler")
	}

	seedPeerClusters := []models.SeedPeerCluster{}
	if err := s.db.WithContext(ctx).Model(&schedulerCluster).Association("SeedPeerClusters").Find(&seedPeerClusters); err != nil {
		return err
	}

	// Delete cluster with transaction.
	tx := s.db.WithContext(ctx).Begin()
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()

	if err := tx.Error; err != nil {
		return err
	}

	for _, seedPeerCluster := range seedPeerClusters {
		if len(seedPeerCluster.SeedPeers) != 0 {
			tx.Rollback()
			return errors.New("seed peer cluster exists seed peer")
		}

		if err := tx.WithContext(ctx).Unscoped().Delete(&models.SeedPeerCluster{}, seedPeerCluster.ID).Error; err != nil {
			tx.Rollback()
			return err
		}
	}

	if err := tx.WithContext(ctx).Model(&schedulerCluster).Association("SeedPeerClusters").Clear(); err != nil {
		tx.Rollback()
		return err
	}

	if err := tx.WithContext(ctx).Unscoped().Delete(&models.SchedulerCluster{}, id).Error; err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit().Error
}

func (s *service) UpdateCluster(ctx context.Context, id uint, json types.UpdateClusterRequest) (*types.UpdateClusterResponse, error) {
	var (
		schedulerClusterConfig map[string]any
		err                    error
	)
	if json.SchedulerClusterConfig != nil {
		schedulerClusterConfig, err = structure.StructToMap(json.SchedulerClusterConfig)
		if err != nil {
			return nil, err
		}
	}

	var seedPeerClusterConfig map[string]any
	if json.SeedPeerClusterConfig != nil {
		seedPeerClusterConfig, err = structure.StructToMap(json.SeedPeerClusterConfig)
		if err != nil {
			return nil, err
		}
	}

	var peerClusterConfig map[string]any
	if json.PeerClusterConfig != nil {
		peerClusterConfig, err = structure.StructToMap(json.PeerClusterConfig)
		if err != nil {
			return nil, err
		}
	}

	var scopes map[string]any
	if json.Scopes != nil {
		scopes, err = structure.StructToMap(json.Scopes)
		if err != nil {
			return nil, err
		}
	}

	// Update cluster with transaction.
	tx := s.db.WithContext(ctx).Begin()
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()

	if err := tx.Error; err != nil {
		return nil, err
	}

	schedulerCluster := models.SchedulerCluster{}
	if err := tx.WithContext(ctx).Preload("SeedPeerClusters").First(&schedulerCluster, id).Updates(models.SchedulerCluster{
		Name:         json.Name,
		BIO:          json.BIO,
		Config:       schedulerClusterConfig,
		ClientConfig: peerClusterConfig,
		Scopes:       scopes,
	}).Error; err != nil {
		tx.Rollback()
		return nil, err
	}

	// Updates does not accept bool as false.
	// Refer to https://stackoverflow.com/questions/56653423/gorm-doesnt-update-boolean-field-to-false.
	if json.IsDefault != schedulerCluster.IsDefault {
		if err := tx.WithContext(ctx).First(&models.SchedulerCluster{}, id).Update("is_default", json.IsDefault).Error; err != nil {
			tx.Rollback()
			return nil, err
		}
	}

	if len(schedulerCluster.SeedPeerClusters) != 1 {
		tx.Rollback()
		return nil, errors.New("invalid number of the seed peer cluster")
	}

	seedPeerCluster := models.SeedPeerCluster{}
	if err := tx.WithContext(ctx).First(&seedPeerCluster, schedulerCluster.SeedPeerClusters[0].ID).Updates(models.SeedPeerCluster{
		Name:   json.Name,
		BIO:    json.BIO,
		Config: seedPeerClusterConfig,
	}).Error; err != nil {
		tx.Rollback()
		return nil, err
	}

	if tx.Commit().Error != nil {
		return nil, err
	}

	return &types.UpdateClusterResponse{
		ID:                     schedulerCluster.ID,
		Name:                   schedulerCluster.Name,
		BIO:                    schedulerCluster.BIO,
		Scopes:                 json.Scopes,
		SchedulerClusterID:     schedulerCluster.ID,
		SeedPeerClusterID:      seedPeerCluster.ID,
		SchedulerClusterConfig: json.SchedulerClusterConfig,
		SeedPeerClusterConfig:  json.SeedPeerClusterConfig,
		PeerClusterConfig:      json.PeerClusterConfig,
		CreatedAt:              schedulerCluster.CreatedAt,
		UpdatedAt:              schedulerCluster.UpdatedAt,
		IsDefault:              json.IsDefault,
	}, nil
}

func (s *service) GetCluster(ctx context.Context, id uint) (*types.GetClusterResponse, error) {
	schedulerCluster := models.SchedulerCluster{}
	if err := s.db.WithContext(ctx).Preload("SeedPeerClusters").First(&schedulerCluster, id).Error; err != nil {
		return nil, err
	}

	if len(schedulerCluster.SeedPeerClusters) != 1 {
		return nil, errors.New("invalid number of the seed peer cluster")
	}

	scopes := &types.SchedulerClusterScopes{}
	if err := structure.MapToStruct(schedulerCluster.Scopes, &scopes); err != nil {
		return nil, err
	}

	schedulerClusterConfig := &types.SchedulerClusterConfig{}
	if err := structure.MapToStruct(schedulerCluster.Config, &schedulerClusterConfig); err != nil {
		return nil, err
	}

	seedPeerClusterConfig := &types.SeedPeerClusterConfig{}
	if err := structure.MapToStruct(schedulerCluster.SeedPeerClusters[0].Config, &seedPeerClusterConfig); err != nil {
		return nil, err
	}

	peerClusterConfig := &types.SchedulerClusterClientConfig{}
	if err := structure.MapToStruct(schedulerCluster.ClientConfig, &peerClusterConfig); err != nil {
		return nil, err
	}

	return &types.GetClusterResponse{
		ID:                     schedulerCluster.ID,
		Name:                   schedulerCluster.Name,
		BIO:                    schedulerCluster.BIO,
		Scopes:                 scopes,
		SchedulerClusterID:     schedulerCluster.ID,
		SeedPeerClusterID:      schedulerCluster.SeedPeerClusters[0].ID,
		SchedulerClusterConfig: schedulerClusterConfig,
		SeedPeerClusterConfig:  seedPeerClusterConfig,
		PeerClusterConfig:      peerClusterConfig,
		CreatedAt:              schedulerCluster.CreatedAt,
		UpdatedAt:              schedulerCluster.UpdatedAt,
		IsDefault:              schedulerCluster.IsDefault,
	}, nil
}

func (s *service) GetClusters(ctx context.Context, q types.GetClustersQuery) ([]types.GetClusterResponse, int64, error) {
	var count int64
	var schedulerClusters []models.SchedulerCluster
	if err := s.db.WithContext(ctx).Scopes(models.Paginate(q.Page, q.PerPage)).Where(&models.SchedulerCluster{
		Name: q.Name,
	}).Preload("SeedPeerClusters").Find(&schedulerClusters).Limit(-1).Offset(-1).Count(&count).Error; err != nil {
		return nil, 0, err
	}

	var resp []types.GetClusterResponse
	for _, schedulerCluster := range schedulerClusters {
		if len(schedulerCluster.SeedPeerClusters) != 1 {
			return nil, 0, errors.New("invalid number of the seed peer cluster")
		}

		scopes := &types.SchedulerClusterScopes{}
		if err := structure.MapToStruct(schedulerCluster.Scopes, &scopes); err != nil {
			return nil, 0, err
		}

		schedulerClusterConfig := &types.SchedulerClusterConfig{}
		if err := structure.MapToStruct(schedulerCluster.Config, &schedulerClusterConfig); err != nil {
			return nil, 0, err
		}

		seedPeerClusterConfig := &types.SeedPeerClusterConfig{}
		if err := structure.MapToStruct(schedulerCluster.SeedPeerClusters[0].Config, &seedPeerClusterConfig); err != nil {
			return nil, 0, err
		}

		peerClusterConfig := &types.SchedulerClusterClientConfig{}
		if err := structure.MapToStruct(schedulerCluster.ClientConfig, &peerClusterConfig); err != nil {
			return nil, 0, err
		}

		resp = append(resp, types.GetClusterResponse{
			ID:                     schedulerCluster.ID,
			Name:                   schedulerCluster.Name,
			BIO:                    schedulerCluster.BIO,
			Scopes:                 scopes,
			SchedulerClusterID:     schedulerCluster.ID,
			SeedPeerClusterID:      schedulerCluster.SeedPeerClusters[0].ID,
			SchedulerClusterConfig: schedulerClusterConfig,
			SeedPeerClusterConfig:  seedPeerClusterConfig,
			PeerClusterConfig:      peerClusterConfig,
			CreatedAt:              schedulerCluster.CreatedAt,
			UpdatedAt:              schedulerCluster.UpdatedAt,
			IsDefault:              schedulerCluster.IsDefault,
		})
	}

	return resp, count, nil
}
