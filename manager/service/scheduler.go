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

func (s *service) CreateScheduler(ctx context.Context, json types.CreateSchedulerRequest) (*models.Scheduler, error) {
	features := types.DefaultSchedulerFeatures
	if json.Features != nil {
		features = json.Features
	}

	scheduler := models.Scheduler{
		Hostname:           json.Hostname,
		IDC:                json.IDC,
		Location:           json.Location,
		IP:                 json.IP,
		Port:               json.Port,
		Features:           features,
		SchedulerClusterID: json.SchedulerClusterID,
	}

	if err := s.db.WithContext(ctx).Create(&scheduler).Error; err != nil {
		return nil, err
	}

	return &scheduler, nil
}

func (s *service) DestroyScheduler(ctx context.Context, id uint) error {
	scheduler := models.Scheduler{}
	if err := s.db.WithContext(ctx).First(&scheduler, id).Error; err != nil {
		return err
	}

	if err := s.db.WithContext(ctx).Unscoped().Unscoped().Delete(&models.Scheduler{}, id).Error; err != nil {
		return err
	}

	return nil
}

func (s *service) UpdateScheduler(ctx context.Context, id uint, json types.UpdateSchedulerRequest) (*models.Scheduler, error) {
	scheduler := models.Scheduler{}
	if err := s.db.WithContext(ctx).First(&scheduler, id).Updates(models.Scheduler{
		IDC:                json.IDC,
		Location:           json.Location,
		IP:                 json.IP,
		Port:               json.Port,
		Features:           json.Features,
		SchedulerClusterID: json.SchedulerClusterID,
	}).Error; err != nil {
		return nil, err
	}

	return &scheduler, nil
}

func (s *service) GetScheduler(ctx context.Context, id uint) (*models.Scheduler, error) {
	scheduler := models.Scheduler{}
	if err := s.db.WithContext(ctx).First(&scheduler, id).Error; err != nil {
		return nil, err
	}

	return &scheduler, nil
}

func (s *service) GetSchedulers(ctx context.Context, q types.GetSchedulersQuery) ([]models.Scheduler, int64, error) {
	var count int64
	var schedulers []models.Scheduler
	if err := s.db.WithContext(ctx).Scopes(models.Paginate(q.Page, q.PerPage)).Where(&models.Scheduler{
		Hostname:           q.Hostname,
		IDC:                q.IDC,
		Location:           q.Location,
		IP:                 q.IP,
		State:              q.State,
		SchedulerClusterID: q.SchedulerClusterID,
	}).Find(&schedulers).Limit(-1).Offset(-1).Count(&count).Error; err != nil {
		return nil, 0, err
	}

	return schedulers, count, nil
}
