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
	"time"

	"d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/manager/types"

	machineryv1tasks "github.com/RichardKnop/machinery/v1/tasks"
)

const (
	// V1PreheatingStatusPending is the preheating is waiting for starting
	V1PreheatingStatusPending = "WAITING"

	// V1PreheatingStatusRunning is the preheating is running
	V1PreheatingStatusRunning = "RUNNING"

	// V1PreheatingStatusSuccess is the preheating is success
	V1PreheatingStatusSuccess = "SUCCESS"

	// V1PreheatingStatusFail is the preheating is failed
	V1PreheatingStatusFail = "FAIL"
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

func (s *rest) CreateV1Preheat(json types.CreateV1PreheatRequest) (*types.CreateV1PreheatResponse, error) {
	p, err := s.CreatePreheat(types.CreatePreheatRequest{
		Type:    json.Type,
		URL:     json.URL,
		Filter:  json.Filter,
		Headers: json.Headers,
	})
	if err != nil {
		return nil, err
	}

	return &types.CreateV1PreheatResponse{
		ID: p.ID,
	}, nil
}

func (s *rest) GetV1Preheat(id string) (*types.GetV1PreheatResponse, error) {
	p, err := s.job.GetPreheat(id)
	if err != nil {
		return nil, err
	}

	return &types.GetV1PreheatResponse{
		ID:         p.ID,
		Status:     convertStatus(p.Status),
		StartTime:  p.CreatedAt.String(),
		FinishTime: time.Now().String(),
	}, nil
}

func convertStatus(status string) string {
	switch status {
	case machineryv1tasks.StatePending, machineryv1tasks.StateReceived, machineryv1tasks.StateRetry:
		return V1PreheatingStatusPending
	case machineryv1tasks.StateStarted:
		return V1PreheatingStatusRunning
	case machineryv1tasks.StateSuccess:
		return V1PreheatingStatusSuccess
	case machineryv1tasks.StateFailure:
		return V1PreheatingStatusFail
	}

	return V1PreheatingStatusFail
}
