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
	"time"

	"github.com/google/uuid"

	"d7y.io/dragonfly/v2/manager/cache"
	"d7y.io/dragonfly/v2/manager/models"
	"d7y.io/dragonfly/v2/manager/types"
)

func (s *service) CreateModel(ctx context.Context, params types.CreateModelParams, json types.CreateModelRequest) (*types.Model, error) {
	scheduler := models.Scheduler{}
	if err := s.db.WithContext(ctx).First(&scheduler, params.SchedulerID).Error; err != nil {
		return nil, err
	}

	model := types.Model{
		ID:          json.ID,
		Name:        json.Name,
		VersionID:   json.VersionID,
		SchedulerID: params.SchedulerID,
		Hostname:    json.Hostname,
		IP:          json.IP,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}

	if _, err := s.rdb.Set(ctx, cache.MakeModelKey(scheduler.SchedulerClusterID, scheduler.HostName, scheduler.IP, model.ID), &model, 0).Result(); err != nil {
		return nil, err
	}

	return &model, nil
}

func (s *service) DestroyModel(ctx context.Context, params types.ModelParams) error {
	if _, err := s.GetModel(ctx, params); err != nil {
		return err
	}

	scheduler := models.Scheduler{}
	if err := s.db.WithContext(ctx).First(&scheduler, params.SchedulerID).Error; err != nil {
		return err
	}

	if _, err := s.rdb.Del(ctx, cache.MakeModelKey(scheduler.SchedulerClusterID, scheduler.HostName, scheduler.IP, params.ID)).Result(); err != nil {
		return err
	}

	return nil
}

func (s *service) UpdateModel(ctx context.Context, params types.ModelParams, json types.UpdateModelRequest) (*types.Model, error) {
	scheduler := models.Scheduler{}
	if err := s.db.WithContext(ctx).First(&scheduler, params.SchedulerID).Error; err != nil {
		return nil, err
	}

	model, err := s.GetModel(ctx, params)
	if err != nil {
		return nil, err
	}
	model.VersionID = json.VersionID
	model.UpdatedAt = time.Now()

	if _, err := s.rdb.Set(ctx, cache.MakeModelKey(scheduler.SchedulerClusterID, scheduler.HostName, scheduler.IP, params.ID), model, 0).Result(); err != nil {
		return nil, err
	}

	return model, nil
}

func (s *service) GetModel(ctx context.Context, params types.ModelParams) (*types.Model, error) {
	scheduler := models.Scheduler{}
	if err := s.db.WithContext(ctx).First(&scheduler, params.SchedulerID).Error; err != nil {
		return nil, err
	}

	var model types.Model
	if err := s.rdb.Get(ctx, cache.MakeModelKey(scheduler.SchedulerClusterID, scheduler.HostName, scheduler.IP, params.ID)).Scan(&model); err != nil {
		return nil, err
	}

	return &model, nil
}

func (s *service) GetModels(ctx context.Context, params types.GetModelsParams) ([]*types.Model, error) {
	scheduler := models.Scheduler{}
	if err := s.db.WithContext(ctx).First(&scheduler, params.SchedulerID).Error; err != nil {
		return nil, err
	}

	models := []*types.Model{}
	iter := s.rdb.Scan(ctx, 0, cache.MakeModelKey(scheduler.SchedulerClusterID, scheduler.HostName, scheduler.IP, "*"), 0).Iterator()
	for iter.Next(ctx) {
		var model types.Model
		if err := s.rdb.Get(ctx, iter.Val()).Scan(&model); err != nil {
			return nil, err
		}

		models = append(models, &model)
	}

	return models, nil
}

func (s *service) CreateModelVersion(ctx context.Context, params types.CreateModelVersionParams, json types.CreateModelVersionRequest) (*types.ModelVersion, error) {
	scheduler := models.Scheduler{}
	if err := s.db.WithContext(ctx).First(&scheduler, params.SchedulerID).Error; err != nil {
		return nil, err
	}

	modelVersion := types.ModelVersion{
		ID:        uuid.New().String(),
		MAE:       json.MAE,
		MSE:       json.MSE,
		RMSE:      json.RMSE,
		R2:        json.R2,
		Data:      json.Data,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	if _, err := s.rdb.Set(ctx, cache.MakeModelVersionKey(scheduler.SchedulerClusterID, scheduler.HostName, scheduler.IP, params.ModelID, modelVersion.ID), &modelVersion, 0).Result(); err != nil {
		return nil, err
	}

	return &modelVersion, nil
}

func (s *service) DestroyModelVersion(ctx context.Context, params types.ModelVersionParams) error {
	if _, err := s.GetModelVersion(ctx, params); err != nil {
		return err
	}

	scheduler := models.Scheduler{}
	if err := s.db.WithContext(ctx).First(&scheduler, params.SchedulerID).Error; err != nil {
		return err
	}

	if _, err := s.rdb.Del(ctx, cache.MakeModelVersionKey(scheduler.SchedulerClusterID, scheduler.HostName, scheduler.IP, params.ModelID, params.ID)).Result(); err != nil {
		return err
	}

	return nil
}

func (s *service) UpdateModelVersion(ctx context.Context, params types.ModelVersionParams, json types.UpdateModelVersionRequest) (*types.ModelVersion, error) {
	scheduler := models.Scheduler{}
	if err := s.db.WithContext(ctx).First(&scheduler, params.SchedulerID).Error; err != nil {
		return nil, err
	}

	modelVersion, err := s.GetModelVersion(ctx, params)
	if err != nil {
		return nil, err
	}

	if json.MAE > 0 {
		modelVersion.MAE = json.MAE
	}

	if json.MSE > 0 {
		modelVersion.MSE = json.MSE
	}

	if json.RMSE > 0 {
		modelVersion.RMSE = json.RMSE
	}

	if json.R2 > 0 {
		modelVersion.R2 = json.R2
	}

	if len(json.Data) > 0 {
		modelVersion.Data = json.Data
	}

	modelVersion.UpdatedAt = time.Now()

	if _, err := s.rdb.Set(ctx, cache.MakeModelVersionKey(scheduler.SchedulerClusterID, scheduler.HostName, scheduler.IP, params.ModelID, modelVersion.ID), modelVersion, 0).Result(); err != nil {
		return nil, err
	}

	return modelVersion, nil
}

func (s *service) GetModelVersion(ctx context.Context, params types.ModelVersionParams) (*types.ModelVersion, error) {
	scheduler := models.Scheduler{}
	if err := s.db.WithContext(ctx).First(&scheduler, params.SchedulerID).Error; err != nil {
		return nil, err
	}

	var modelVersion types.ModelVersion
	if err := s.rdb.Get(ctx, cache.MakeModelVersionKey(scheduler.SchedulerClusterID, scheduler.HostName, scheduler.IP, params.ModelID, params.ID)).Scan(&modelVersion); err != nil {
		return nil, err
	}

	return &modelVersion, nil
}

func (s *service) GetModelVersions(ctx context.Context, params types.GetModelVersionsParams) ([]*types.ModelVersion, error) {
	scheduler := models.Scheduler{}
	if err := s.db.WithContext(ctx).First(&scheduler, params.SchedulerID).Error; err != nil {
		return nil, err
	}

	modelVersions := []*types.ModelVersion{}
	iter := s.rdb.Scan(ctx, 0, cache.MakeModelVersionKey(scheduler.SchedulerClusterID, scheduler.HostName, scheduler.IP, params.ModelID, "*"), 0).Iterator()
	for iter.Next(ctx) {
		var modelVersion types.ModelVersion
		if err := s.rdb.Get(ctx, iter.Val()).Scan(&modelVersion); err != nil {
			return nil, err
		}

		modelVersions = append(modelVersions, &modelVersion)
	}

	return modelVersions, nil
}
