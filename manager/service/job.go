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
	"fmt"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/manager/types"
	"d7y.io/dragonfly/v2/pkg/retry"
	"d7y.io/dragonfly/v2/pkg/util/structutils"
	machineryv1tasks "github.com/RichardKnop/machinery/v1/tasks"
)

func (s *rest) CreatePreheatJob(ctx context.Context, json types.CreatePreheatJobRequest) (*model.Job, error) {
	var schedulers []model.Scheduler
	var schedulerClusters []model.SchedulerCluster

	if len(json.SchedulerClusterIDs) != 0 {
		for _, schedulerClusterID := range json.SchedulerClusterIDs {
			schedulerCluster := model.SchedulerCluster{}
			if err := s.db.WithContext(ctx).First(&schedulerCluster, schedulerClusterID).Error; err != nil {
				return nil, err
			}
			schedulerClusters = append(schedulerClusters, schedulerCluster)

			scheduler := model.Scheduler{}
			if err := s.db.WithContext(ctx).First(&scheduler, model.Scheduler{
				SchedulerClusterID: schedulerCluster.ID,
				Status:             model.SchedulerStatusActive,
			}).Error; err != nil {
				return nil, err
			}
			schedulers = append(schedulers, scheduler)
		}
	} else {
		if err := s.db.WithContext(ctx).Find(&schedulerClusters).Error; err != nil {
			return nil, err
		}

		for _, schedulerCluster := range schedulerClusters {
			scheduler := model.Scheduler{}
			if err := s.db.WithContext(ctx).First(&scheduler, model.Scheduler{
				SchedulerClusterID: schedulerCluster.ID,
				Status:             model.SchedulerStatusActive,
			}).Error; err != nil {
				continue
			}

			schedulers = append(schedulers, scheduler)
		}
	}

	groupJobState, err := s.job.CreatePreheat(ctx, schedulers, json.Args)
	if err != nil {
		return nil, err
	}

	args, err := structutils.StructToMap(json.Args)
	if err != nil {
		return nil, err
	}

	job := model.Job{
		TaskID:            groupJobState.GroupUUID,
		BIO:               json.BIO,
		Type:              json.Type,
		Status:            groupJobState.State,
		Args:              args,
		UserID:            json.UserID,
		SchedulerClusters: schedulerClusters,
	}

	if err := s.db.WithContext(ctx).Create(&job).Error; err != nil {
		return nil, err
	}

	go s.pollingJob(context.Background(), job.ID, job.TaskID)

	return &job, nil
}

func (s *rest) pollingJob(ctx context.Context, id uint, taskID string) {
	var job model.Job

	if _, _, err := retry.Run(ctx, func() (interface{}, bool, error) {
		groupJob, err := s.job.GetGroupJobState(taskID)
		if err != nil {
			logger.Errorf("polling job %d and task %s failed: %v", id, taskID, err)
			return nil, false, err
		}

		if err := s.db.WithContext(ctx).First(&job, id).Updates(model.Job{
			Status: groupJob.State,
		}).Error; err != nil {
			logger.Errorf("polling job %d and task %s store failed: %v", id, taskID, err)
			return nil, true, err
		}

		switch job.Status {
		case machineryv1tasks.StateSuccess:
			logger.Infof("polling job %d and task %s is finally successful", id, taskID)
			return nil, true, nil
		case machineryv1tasks.StateFailure:
			logger.Errorf("polling job %d and task %s is finally failed", id, taskID)
			return nil, true, nil
		default:
			return nil, false, fmt.Errorf("polling job %d and task %s status is %s", id, taskID, job.Status)
		}
	}, 5, 10, 120, nil); err != nil {
		logger.Errorf("polling job %d and task %s failed %s", id, taskID, err)
	}

	// Polling timeout and failed
	if job.Status != machineryv1tasks.StateSuccess && job.Status != machineryv1tasks.StateFailure {
		job := model.Job{}
		if err := s.db.WithContext(ctx).First(&job, id).Updates(model.Job{
			Status: machineryv1tasks.StateFailure,
		}).Error; err != nil {
			logger.Errorf("polling job %d and task %s store failed: %v", id, taskID, err)
		}
		logger.Errorf("polling job %d and task %s timeout", id, taskID)
	}
}

func (s *rest) DestroyJob(ctx context.Context, id uint) error {
	job := model.Job{}
	if err := s.db.WithContext(ctx).First(&job, id).Error; err != nil {
		return err
	}

	if err := s.db.WithContext(ctx).Unscoped().Delete(&model.Job{}, id).Error; err != nil {
		return err
	}

	return nil
}

func (s *rest) UpdateJob(ctx context.Context, id uint, json types.UpdateJobRequest) (*model.Job, error) {
	job := model.Job{}
	if err := s.db.WithContext(ctx).Preload("CDNClusters").Preload("SchedulerClusters").First(&job, id).Updates(model.Job{
		BIO:    json.BIO,
		UserID: json.UserID,
	}).Error; err != nil {
		return nil, err
	}

	return &job, nil
}

func (s *rest) GetJob(ctx context.Context, id uint) (*model.Job, error) {
	job := model.Job{}
	if err := s.db.WithContext(ctx).Preload("CDNClusters").Preload("SchedulerClusters").First(&job, id).Error; err != nil {
		return nil, err
	}

	return &job, nil
}

func (s *rest) GetJobs(ctx context.Context, q types.GetJobsQuery) (*[]model.Job, int64, error) {
	var count int64
	var jobs []model.Job
	if err := s.db.WithContext(ctx).Scopes(model.Paginate(q.Page, q.PerPage)).Where(&model.Job{
		Type:   q.Type,
		Status: q.Status,
		UserID: q.UserID,
	}).Find(&jobs).Count(&count).Error; err != nil {
		return nil, 0, err
	}

	return &jobs, count, nil
}

func (s *rest) AddJobToSchedulerClusters(ctx context.Context, id, schedulerClusterIDs []uint) error {
	job := model.Job{}
	if err := s.db.WithContext(ctx).First(&job, id).Error; err != nil {
		return err
	}

	var schedulerClusters []*model.SchedulerCluster
	for _, schedulerClusterID := range schedulerClusterIDs {
		schedulerCluster := model.SchedulerCluster{}
		if err := s.db.WithContext(ctx).First(&schedulerCluster, schedulerClusterID).Error; err != nil {
			return err
		}
		schedulerClusters = append(schedulerClusters, &schedulerCluster)
	}

	if err := s.db.WithContext(ctx).Model(&job).Association("SchedulerClusters").Append(schedulerClusters); err != nil {
		return err
	}

	return nil
}

func (s *rest) AddJobToCDNClusters(ctx context.Context, id, cdnClusterIDs []uint) error {
	job := model.Job{}
	if err := s.db.WithContext(ctx).First(&job, id).Error; err != nil {
		return err
	}

	var cdnClusters []*model.CDNCluster
	for _, cdnClusterID := range cdnClusterIDs {
		cdnCluster := model.CDNCluster{}
		if err := s.db.WithContext(ctx).First(&cdnCluster, cdnClusterID).Error; err != nil {
			return err
		}
		cdnClusters = append(cdnClusters, &cdnCluster)
	}

	if err := s.db.WithContext(ctx).Model(&job).Association("CDNClusters").Append(cdnClusters); err != nil {
		return err
	}

	return nil
}
