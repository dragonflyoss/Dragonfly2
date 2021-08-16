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

func (s *rest) CreatePreheat(json types.CreatePreheatRequest) (*types.Preheat, error) {
	if json.SchedulerClusterID != nil {
		schedulerCluster := model.SchedulerCluster{}
		if err := s.db.First(&schedulerCluster, json.SchedulerClusterID).Error; err != nil {
			return nil, err
		}

		scheduler := model.Scheduler{}
		if err := s.db.First(&scheduler, model.Scheduler{
			SchedulerClusterID: schedulerCluster.ID,
			Status:             model.SchedulerStatusActive,
		}).Error; err != nil {
			return nil, err
		}

		return s.job.CreatePreheat([]string{scheduler.HostName}, json)
	}

	schedulerClusters := []model.SchedulerCluster{}
	if err := s.db.Find(&schedulerClusters).Error; err != nil {
		return nil, err
	}

	hostnames := []string{}
	for _, schedulerCluster := range schedulerClusters {
		scheduler := model.Scheduler{}
		if err := s.db.First(&scheduler, model.Scheduler{
			SchedulerClusterID: schedulerCluster.ID,
			Status:             model.SchedulerStatusActive,
		}).Error; err != nil {
			continue
		}

		hostnames = append(hostnames, scheduler.HostName)
	}

	return s.job.CreatePreheat(hostnames, json)
}

func (s *rest) GetPreheat(id string) (*types.Preheat, error) {
	return s.job.GetPreheat(id)
}
