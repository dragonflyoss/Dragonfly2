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

func (s *rest) CreateScheduler(json types.CreateSchedulerRequest) (*model.Scheduler, error) {
	scheduler := model.Scheduler{
		HostName:           json.HostName,
		VIPs:               json.VIPs,
		IDC:                json.IDC,
		Location:           json.Location,
		NetConfig:          json.NetConfig,
		IP:                 json.IP,
		Port:               json.Port,
		SchedulerClusterID: json.SchedulerClusterID,
	}

	if err := s.db.Create(&scheduler).Error; err != nil {
		return nil, err
	}

	return &scheduler, nil
}

func (s *rest) DestroyScheduler(id uint) error {
	scheduler := model.Scheduler{}
	if err := s.db.First(&scheduler, id).Error; err != nil {
		return err
	}

	if err := s.db.Unscoped().Delete(&model.Scheduler{}, id).Error; err != nil {
		return err
	}

	return nil
}

func (s *rest) UpdateScheduler(id uint, json types.UpdateSchedulerRequest) (*model.Scheduler, error) {
	scheduler := model.Scheduler{}
	if err := s.db.First(&scheduler, id).Updates(model.Scheduler{
		VIPs:               json.VIPs,
		IDC:                json.IDC,
		Location:           json.Location,
		NetConfig:          json.NetConfig,
		IP:                 json.IP,
		Port:               json.Port,
		SchedulerClusterID: json.SchedulerClusterID,
	}).Error; err != nil {
		return nil, err
	}

	return &scheduler, nil
}

func (s *rest) GetScheduler(id uint) (*model.Scheduler, error) {
	scheduler := model.Scheduler{}
	if err := s.db.First(&scheduler, id).Error; err != nil {
		return nil, err
	}

	return &scheduler, nil
}

func (s *rest) GetSchedulers(q types.GetSchedulersQuery) (*[]model.Scheduler, error) {
	schedulers := []model.Scheduler{}
	if err := s.db.Scopes(model.Paginate(q.Page, q.PerPage)).Where(&model.Scheduler{
		HostName:           q.HostName,
		IDC:                q.IDC,
		Location:           q.Location,
		IP:                 q.IP,
		Status:             q.Status,
		SchedulerClusterID: q.SchedulerClusterID,
	}).Find(&schedulers).Error; err != nil {
		return nil, err
	}

	return &schedulers, nil
}

func (s *rest) SchedulerTotalCount(q types.GetSchedulersQuery) (int64, error) {
	var count int64
	if err := s.db.Model(&model.Scheduler{}).Where(&model.Scheduler{
		HostName:           q.HostName,
		IDC:                q.IDC,
		Location:           q.Location,
		IP:                 q.IP,
		Status:             q.Status,
		SchedulerClusterID: q.SchedulerClusterID,
	}).Count(&count).Error; err != nil {
		return 0, err
	}

	return count, nil
}
