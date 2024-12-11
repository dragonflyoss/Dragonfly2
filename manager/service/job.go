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
	internaljob "d7y.io/dragonfly/v2/internal/job"
	"d7y.io/dragonfly/v2/manager/metrics"
	"d7y.io/dragonfly/v2/manager/models"
	"d7y.io/dragonfly/v2/manager/types"
	"d7y.io/dragonfly/v2/pkg/net/http"
	"d7y.io/dragonfly/v2/pkg/retry"
	"d7y.io/dragonfly/v2/pkg/slices"
	"d7y.io/dragonfly/v2/pkg/structure"
)

func (s *service) CreateSyncPeersJob(ctx context.Context, json types.CreateSyncPeersJobRequest) error {
	schedulers, err := s.findSchedulerInClusters(ctx, json.SchedulerClusterIDs)
	if err != nil {
		return err
	}

	return s.job.SyncPeers.CreateSyncPeers(ctx, schedulers)
}

func (s *service) CreatePreheatJob(ctx context.Context, json types.CreatePreheatJobRequest) (*models.Job, error) {
	if json.Args.Scope == "" {
		json.Args.Scope = types.SingleSeedPeerScope
	}

	if json.Args.ConcurrentCount == 0 {
		json.Args.ConcurrentCount = types.DefaultPreheatConcurrentCount
	}

	if json.Args.Timeout == 0 {
		json.Args.Timeout = types.DefaultJobTimeout
	}

	if json.Args.FilteredQueryParams == "" {
		json.Args.FilteredQueryParams = http.RawDefaultFilteredQueryParams
	}

	args, err := structure.StructToMap(json.Args)
	if err != nil {
		return nil, err
	}

	candidateSchedulers, err := s.findAllCandidateSchedulersInClusters(ctx, json.SchedulerClusterIDs, []string{types.SchedulerFeaturePreheat})
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

	go s.pollingJob(context.Background(), internaljob.PreheatJob, job.ID, job.TaskID)
	return &job, nil
}

func (s *service) CreateGetTaskJob(ctx context.Context, json types.CreateGetTaskJobRequest) (*models.Job, error) {
	if json.Args.FilteredQueryParams == "" {
		json.Args.FilteredQueryParams = http.RawDefaultFilteredQueryParams
	}

	args, err := structure.StructToMap(json.Args)
	if err != nil {
		return nil, err
	}

	schedulers, err := s.findAllSchedulersInClusters(ctx, json.SchedulerClusterIDs)
	if err != nil {
		return nil, err
	}

	groupJobState, err := s.job.CreateGetTask(ctx, schedulers, json.Args)
	if err != nil {
		return nil, err
	}

	var schedulerClusters []models.SchedulerCluster
	for _, scheduler := range schedulers {
		schedulerClusters = append(schedulerClusters, scheduler.SchedulerCluster)
	}

	job := models.Job{
		TaskID:            groupJobState.GroupUUID,
		BIO:               json.BIO,
		Type:              json.Type,
		State:             groupJobState.State,
		Args:              args,
		UserID:            json.UserID,
		SchedulerClusters: schedulerClusters,
	}

	if err := s.db.WithContext(ctx).Create(&job).Error; err != nil {
		return nil, err
	}

	go s.pollingJob(context.Background(), internaljob.GetTaskJob, job.ID, job.TaskID)
	return &job, nil
}

func (s *service) CreateDeleteTaskJob(ctx context.Context, json types.CreateDeleteTaskJobRequest) (*models.Job, error) {
	if json.Args.Timeout == 0 {
		json.Args.Timeout = types.DefaultJobTimeout
	}

	if json.Args.FilteredQueryParams == "" {
		json.Args.FilteredQueryParams = http.RawDefaultFilteredQueryParams
	}

	args, err := structure.StructToMap(json.Args)
	if err != nil {
		return nil, err
	}

	schedulers, err := s.findAllSchedulersInClusters(ctx, json.SchedulerClusterIDs)
	if err != nil {
		return nil, err
	}

	groupJobState, err := s.job.CreateDeleteTask(ctx, schedulers, json.Args)
	if err != nil {
		return nil, err
	}

	var schedulerClusters []models.SchedulerCluster
	for _, scheduler := range schedulers {
		schedulerClusters = append(schedulerClusters, scheduler.SchedulerCluster)
	}

	job := models.Job{
		TaskID:            groupJobState.GroupUUID,
		BIO:               json.BIO,
		Type:              json.Type,
		State:             groupJobState.State,
		Args:              args,
		UserID:            json.UserID,
		SchedulerClusters: schedulerClusters,
	}

	if err := s.db.WithContext(ctx).Create(&job).Error; err != nil {
		return nil, err
	}

	go s.pollingJob(context.Background(), internaljob.DeleteTaskJob, job.ID, job.TaskID)
	return &job, nil
}

func (s *service) findSchedulerInClusters(ctx context.Context, schedulerClusterIDs []uint) ([]models.Scheduler, error) {
	var activeSchedulers []models.Scheduler
	if len(schedulerClusterIDs) != 0 {
		// Find the scheduler clusters by request.
		for _, schedulerClusterID := range schedulerClusterIDs {
			schedulerCluster := models.SchedulerCluster{}
			if err := s.db.WithContext(ctx).First(&schedulerCluster, schedulerClusterID).Error; err != nil {
				return nil, fmt.Errorf("scheduler cluster id %d: %w", schedulerClusterID, err)
			}

			scheduler := models.Scheduler{}
			if err := s.db.WithContext(ctx).Preload("SchedulerCluster").First(&scheduler, models.Scheduler{
				SchedulerClusterID: schedulerCluster.ID,
				State:              models.SchedulerStateActive,
			}).Error; err != nil {
				return nil, fmt.Errorf("scheduler cluster id %d: %w", schedulerClusterID, err)
			}

			activeSchedulers = append(activeSchedulers, scheduler)
		}
	} else {
		// Find all of the scheduler clusters that has active scheduler.
		var schedulerClusters []models.SchedulerCluster
		if err := s.db.WithContext(ctx).Find(&schedulerClusters).Error; err != nil {
			return nil, err
		}

		for _, schedulerCluster := range schedulerClusters {
			scheduler := models.Scheduler{}
			if err := s.db.WithContext(ctx).Preload("SchedulerCluster").First(&scheduler, models.Scheduler{
				SchedulerClusterID: schedulerCluster.ID,
				State:              models.SchedulerStateActive,
			}).Error; err != nil {
				continue
			}

			activeSchedulers = append(activeSchedulers, scheduler)
		}
	}

	if len(activeSchedulers) == 0 {
		return nil, errors.New("active schedulers not found")
	}

	return activeSchedulers, nil
}

func (s *service) findAllSchedulersInClusters(ctx context.Context, schedulerClusterIDs []uint) ([]models.Scheduler, error) {
	var activeSchedulers []models.Scheduler
	if len(schedulerClusterIDs) != 0 {
		// Find the scheduler clusters by request.
		for _, schedulerClusterID := range schedulerClusterIDs {
			schedulerCluster := models.SchedulerCluster{}
			if err := s.db.WithContext(ctx).First(&schedulerCluster, schedulerClusterID).Error; err != nil {
				return nil, fmt.Errorf("scheduler cluster id %d: %w", schedulerClusterID, err)
			}

			var schedulers []models.Scheduler
			if err := s.db.WithContext(ctx).Preload("SchedulerCluster").Find(&schedulers, models.Scheduler{
				SchedulerClusterID: schedulerCluster.ID,
				State:              models.SchedulerStateActive,
			}).Error; err != nil {
				return nil, fmt.Errorf("scheduler cluster id %d: %w", schedulerClusterID, err)
			}

			activeSchedulers = append(activeSchedulers, schedulers...)
		}
	} else {
		// Find all of the scheduler clusters that has active schedulers.
		var schedulerClusters []models.SchedulerCluster
		if err := s.db.WithContext(ctx).Find(&schedulerClusters).Error; err != nil {
			return nil, err
		}

		for _, schedulerCluster := range schedulerClusters {
			var schedulers []models.Scheduler
			if err := s.db.WithContext(ctx).Preload("SchedulerCluster").Find(&schedulers, models.Scheduler{
				SchedulerClusterID: schedulerCluster.ID,
				State:              models.SchedulerStateActive,
			}).Error; err != nil {
				continue
			}

			activeSchedulers = append(activeSchedulers, schedulers...)
		}
	}

	if len(activeSchedulers) == 0 {
		return nil, errors.New("active schedulers not found")
	}

	return activeSchedulers, nil
}

func (s *service) findAllCandidateSchedulersInClusters(ctx context.Context, schedulerClusterIDs []uint, features []string) ([]models.Scheduler, error) {
	var candidateSchedulers []models.Scheduler
	if len(schedulerClusterIDs) != 0 {
		// Find the scheduler clusters by request.
		for _, schedulerClusterID := range schedulerClusterIDs {
			schedulerCluster := models.SchedulerCluster{}
			if err := s.db.WithContext(ctx).First(&schedulerCluster, schedulerClusterID).Error; err != nil {
				return nil, fmt.Errorf("scheduler cluster id %d: %w", schedulerClusterID, err)
			}

			var schedulers []models.Scheduler
			if err := s.db.WithContext(ctx).Preload("SchedulerCluster").Find(&schedulers, models.Scheduler{
				SchedulerClusterID: schedulerCluster.ID,
				State:              models.SchedulerStateActive,
			}).Error; err != nil {
				return nil, fmt.Errorf("scheduler cluster id %d: %w", schedulerClusterID, err)
			}

			for _, scheduler := range schedulers {
				// If the features is empty, return the first scheduler directly.
				if len(features) == 0 {
					candidateSchedulers = append(candidateSchedulers, scheduler)
					break
				}

				// Scan the schedulers to find the first scheduler that supports feature.
				if slices.Contains(scheduler.Features, features...) {
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

			for _, scheduler := range schedulers {
				// If the features is empty, return the first scheduler directly.
				if len(features) == 0 {
					candidateSchedulers = append(candidateSchedulers, scheduler)
					break
				}

				// Scan the schedulers to find the first scheduler that supports feature.
				if slices.Contains(scheduler.Features, features...) {
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

func (s *service) pollingJob(ctx context.Context, name string, id uint, groupID string) {
	var (
		job models.Job
		log = logger.WithGroupAndJobID(groupID, fmt.Sprint(id))
	)
	if _, _, err := retry.Run(ctx, 30, 300, 16, func() (any, bool, error) {
		groupJob, err := s.job.GetGroupJobState(name, groupID)
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
			// Collect CreateJobSuccessCount. metrics.
			metrics.CreateJobSuccessCount.WithLabelValues(name).Inc()

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
