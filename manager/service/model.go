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
	"encoding/json"

	"d7y.io/dragonfly/v2/manager/cache"
	"d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/manager/types"
)

func (s *service) DestroyModel(ctx context.Context, params types.ModelParams) error {
	scheduler := model.Scheduler{}
	if err := s.db.WithContext(ctx).First(&scheduler, params.SchedulerID).Error; err != nil {
		return err
	}

	if _, err := s.rdb.Del(ctx, cache.MakeModelKey(scheduler.SchedulerClusterID, scheduler.HostName, scheduler.IP, params.ID)).Result(); err != nil {
		return err
	}

	return nil
}

func (s *service) UpdateModel(ctx context.Context, params types.ModelParams, json types.UpdateModelRequest) (*types.Model, error) {
	scheduler := model.Scheduler{}
	if err := s.db.WithContext(ctx).First(&scheduler, params.SchedulerID).Error; err != nil {
		return nil, err
	}

	model, err := s.GetModel(ctx, params)
	if err != nil {
		return nil, err
	}
	model.VersionID = json.VersionID

	if _, err := s.rdb.Set(ctx, cache.MakeModelKey(scheduler.SchedulerClusterID, scheduler.HostName, scheduler.IP, params.ID), model, 0).Result(); err != nil {
		return nil, err
	}

	return model, nil
}

func (s *service) GetModel(ctx context.Context, params types.ModelParams) (*types.Model, error) {
	scheduler := model.Scheduler{}
	if err := s.db.WithContext(ctx).First(&scheduler, params.SchedulerID).Error; err != nil {
		return nil, err
	}

	b, err := s.rdb.Get(ctx, cache.MakeModelKey(scheduler.SchedulerClusterID, scheduler.HostName, scheduler.IP, params.ID)).Bytes()
	if err != nil {
		return nil, err
	}

	var model types.Model
	if err = json.Unmarshal(b, &model); err != nil {
		return nil, err
	}

	return &model, nil
}

func (s *service) GetModels(ctx context.Context, params types.GetModelsParams) ([]*types.Model, error) {
	scheduler := model.Scheduler{}
	if err := s.db.WithContext(ctx).First(&scheduler, params.SchedulerID).Error; err != nil {
		return nil, err
	}

	var models []*types.Model
	iter := s.rdb.Scan(ctx, 0, cache.MakeModelKey(scheduler.SchedulerClusterID, scheduler.HostName, scheduler.IP, "*"), 0).Iterator()
	for iter.Next(ctx) {
		b, err := s.rdb.Get(ctx, iter.Val()).Bytes()
		if err != nil {
			return nil, err
		}

		var model types.Model
		if err = json.Unmarshal(b, &model); err != nil {
			return nil, err
		}

		models = append(models, &model)
	}

	return models, nil
}
