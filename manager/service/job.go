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
	"errors"
	"fmt"

	machineryv1tasks "github.com/RichardKnop/machinery/v1/tasks"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/manager/models"
	"d7y.io/dragonfly/v2/manager/types"
	"d7y.io/dragonfly/v2/pkg/retry"
	"d7y.io/dragonfly/v2/pkg/slices"
	"d7y.io/dragonfly/v2/pkg/structure"
)

func (s *service) CreatePreheatJob(ctx context.Context, json types.CreatePreheatJobRequest) (*models.Job, error) {
	candidateSchedulers, err := s.findCandidateSchedulers(ctx, json.SchedulerClusterIDs)
	if err != nil {
		return nil, err
	}

	groupJobState, err := s.job.CreatePreheat(ctx, candidateSchedulers, json.Args)
	if err != nil {
		return nil, err
	}

	var candidateSchedulerClusters []models.SchedulerCluster
	for _, candidateScheduler := range candidateSchedulers {
		candidateSchedulerClusters = append(candidateSchedulerClusters, candidateScheduler.SchedulerCluster)
	}

	args, err := structure.StructToMap(json.Args)
	if err != nil {
		return nil, err
	}

	job := models.Job{
		TaskID:            groupJobState.GroupUUID,
		BIO:               json.BIO,
		Type:              json.Type,
		State:             groupJobState.State,
		Args:              args,
		UserID:            json.UserID,
		SchedulerClusters: candidateSchedulerClusters,
	}

	if err := s.db.WithContext(ctx).Create(&job).Error; err != nil {
		return nil, err
	}

	go s.pollingJob(context.Background(), job.ID, job.TaskID)

	return &job, nil
}

func (s *service) findCandidateSchedulers(ctx context.Context, schedulerClusterIDs []uint) ([]models.Scheduler, error) {
	var candidateSchedulers []models.Scheduler
	if len(schedulerClusterIDs) != 0 {
		// Find the scheduler clusters by request.
		for _, schedulerClusterID := range schedulerClusterIDs {
			schedulerCluster := models.SchedulerCluster{}
			if err := s.db.WithContext(ctx).First(&schedulerCluster, schedulerClusterID).Error; err != nil {
				return nil, err
			}

			var schedulers []models.Scheduler
			if err := s.db.WithContext(ctx).Preload("SchedulerCluster").Find(&schedulers, models.Scheduler{
				SchedulerClusterID: schedulerCluster.ID,
				State:              models.SchedulerStateActive,
			}).Error; err != nil {
				return nil, err
			}

			// Scan the schedulers to find the first scheduler that supports preheat.
			for _, scheduler := range schedulers {
				if slices.Contains(scheduler.Features, types.SchedulerFeaturePreheat) {
					candidateSchedulers = append(candidateSchedulers, scheduler)
					break
				}
			}
		}
	} else {
		// Find all of the scheduler clusters that has active schedulers.
		var candidateSchedulerClusters []models.SchedulerCluster
		if err := s.db.WithContext(ctx).Find(&candidateSchedulerClusters).Error; err != nil {
			return nil, err
		}

		for _, candidateSchedulerCluster := range candidateSchedulerClusters {
			var schedulers []models.Scheduler
			if err := s.db.WithContext(ctx).Preload("SchedulerCluster").Find(&schedulers, models.Scheduler{
				SchedulerClusterID: candidateSchedulerCluster.ID,
				State:              models.SchedulerStateActive,
			}).Error; err != nil {
				continue
			}

			// Scan the schedulers to find the first scheduler that supports preheat.
			for _, scheduler := range schedulers {
				if slices.Contains(scheduler.Features, types.SchedulerFeaturePreheat) {
					candidateSchedulers = append(candidateSchedulers, scheduler)
					break
				}
			}
		}
	}

	if len(candidateSchedulers) == 0 {
		return nil, errors.New("candidate schedulers not found")
	}

	return candidateSchedulers, nil
}

func (s *service) pollingJob(ctx context.Context, id uint, groupID string) {
	var (
		job models.Job
		log = logger.WithGroupAndJobID(groupID, fmt.Sprint(id))
	)
	if _, _, err := retry.Run(ctx, 5, 10, 480, func() (any, bool, error) {
		groupJob, err := s.job.GetGroupJobState(groupID)
		if err != nil {
			log.Errorf("polling group failed: %s", err.Error())
			return nil, false, err
		}

		result, err := structure.StructToMap(groupJob)
		if err != nil {
			log.Errorf("polling group failed: %s", err.Error())
			return nil, false, err
		}

		if err := s.db.WithContext(ctx).First(&job, id).Updates(models.Job{
			State:  groupJob.State,
			Result: result,
		}).Error; err != nil {
			log.Errorf("polling group failed: %s", err.Error())
			return nil, true, err
		}

		switch job.State {
		case machineryv1tasks.StateSuccess:
			log.Info("polling group succeeded")
			return nil, true, nil
		case machineryv1tasks.StateFailure:
			log.Error("polling group failed")
			return nil, true, nil
		default:
			msg := fmt.Sprintf("polling job state is %s", job.State)
			log.Info(msg)
			return nil, false, errors.New(msg)
		}
	}); err != nil {
		log.Errorf("polling group failed: %s", err.Error())
	}

	// Polling timeout and failed.
	if job.State != machineryv1tasks.StateSuccess && job.State != machineryv1tasks.StateFailure {
		job := models.Job{}
		if err := s.db.WithContext(ctx).First(&job, id).Updates(models.Job{
			State: machineryv1tasks.StateFailure,
		}).Error; err != nil {
			log.Errorf("polling group failed: %s", err.Error())
		}
		log.Error("polling group timeout")
	}
}

func (s *service) DestroyJob(ctx context.Context, id uint) error {
	job := models.Job{}
	if err := s.db.WithContext(ctx).First(&job, id).Error; err != nil {
		return err
	}

	if err := s.db.WithContext(ctx).Unscoped().Delete(&models.Job{}, id).Error; err != nil {
		return err
	}

	return nil
}

func (s *service) UpdateJob(ctx context.Context, id uint, json types.UpdateJobRequest) (*models.Job, error) {
	job := models.Job{}
	if err := s.db.WithContext(ctx).Preload("SeedPeerClusters").Preload("SchedulerClusters").First(&job, id).Updates(models.Job{
		BIO:    json.BIO,
		UserID: json.UserID,
	}).Error; err != nil {
		return nil, err
	}

	return &job, nil
}

func (s *service) GetJob(ctx context.Context, id uint) (*models.Job, error) {
	job := models.Job{}
	if err := s.db.WithContext(ctx).Preload("SeedPeerClusters").Preload("SchedulerClusters").First(&job, id).Error; err != nil {
		return nil, err
	}

	return &job, nil
}

func (s *service) GetJobs(ctx context.Context, q types.GetJobsQuery) ([]models.Job, int64, error) {
	var count int64
	var jobs []models.Job
	if err := s.db.WithContext(ctx).Scopes(models.Paginate(q.Page, q.PerPage)).Where(&models.Job{
		Type:   q.Type,
		State:  q.State,
		UserID: q.UserID,
	}).Order("created_at DESC").Find(&jobs).Limit(-1).Offset(-1).Count(&count).Error; err != nil {
		return nil, 0, err
	}

	return jobs, count, nil
}

func (s *service) AddJobToSchedulerClusters(ctx context.Context, id, schedulerClusterIDs []uint) error {
	job := models.Job{}
	if err := s.db.WithContext(ctx).First(&job, id).Error; err != nil {
		return err
	}

	var schedulerClusters []*models.SchedulerCluster
	for _, schedulerClusterID := range schedulerClusterIDs {
		schedulerCluster := models.SchedulerCluster{}
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

func (s *service) AddJobToSeedPeerClusters(ctx context.Context, id, seedPeerClusterIDs []uint) error {
	job := models.Job{}
	if err := s.db.WithContext(ctx).First(&job, id).Error; err != nil {
		return err
	}

	var seedPeerClusters []*models.SeedPeerCluster
	for _, seedPeerClusterID := range seedPeerClusterIDs {
		seedPeerCluster := models.SeedPeerCluster{}
		if err := s.db.WithContext(ctx).First(&seedPeerCluster, seedPeerClusterID).Error; err != nil {
			return err
		}
		seedPeerClusters = append(seedPeerClusters, &seedPeerCluster)
	}

	if err := s.db.WithContext(ctx).Model(&job).Association("SeedPeerClusters").Append(seedPeerClusters); err != nil {
		return err
	}

	return nil
}
