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

	"d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/manager/types"
	"d7y.io/dragonfly/v2/pkg/util/structutils"
)

func (s *rest) CreatePreheatJob(ctx context.Context, json types.CreatePreheatRequest) (*model.Job, error) {
	var schedulers []model.Scheduler
	if len(json.SchedulerClusterIDs) != 0 {
		for _, schedulerClusterID := range json.SchedulerClusterIDs {
			schedulerCluster := model.SchedulerCluster{}
			if err := s.db.WithContext(ctx).First(&schedulerCluster, schedulerClusterID).Error; err != nil {
				return nil, err
			}

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
		schedulerClusters := []model.SchedulerCluster{}
		if err := s.db.WithContext(ctx).Find(&schedulerClusters).Error; err != nil {
			return nil, err
		}

		var schedulers []model.Scheduler
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

	groupJobState := s.job.CreatePreheat(ctx, schedulers, json.Args)

	job := model.Job{
		BIO:    json.BIO,
		Type:   json.Type,
		Args:   structutils.StructToMap(json.Args),
		UserID: json.UserID,
	}

	if err := s.db.WithContext(ctx).Create(&job).Error; err != nil {
		return nil, err
	}

	return &job, nil
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
	if err := s.db.WithContext(ctx).First(&job, id).Updates(model.Job{
		IDC:          json.IDC,
		Location:     json.Location,
		IP:           json.IP,
		Port:         json.Port,
		DownloadPort: json.DownloadPort,
		JobClusterID: json.JobClusterID,
	}).Error; err != nil {
		return nil, err
	}

	return &job, nil
}

func (s *rest) GetJob(ctx context.Context, id uint) (*model.Job, error) {
	job := model.Job{}
	if err := s.db.WithContext(ctx).First(&job, id).Error; err != nil {
		return nil, err
	}

	return &job, nil
}

func (s *rest) GetJobs(ctx context.Context, q types.GetJobsQuery) (*[]model.Job, error) {
	jobs := []model.Job{}
	if err := s.db.WithContext(ctx).Scopes(model.Paginate(q.Page, q.PerPage)).Where(&model.Job{
		HostName:     q.HostName,
		IDC:          q.IDC,
		Location:     q.Location,
		IP:           q.IP,
		Port:         q.Port,
		DownloadPort: q.DownloadPort,
		JobClusterID: q.JobClusterID,
	}).Find(&jobs).Error; err != nil {
		return nil, err
	}

	return &jobs, nil
}

func (s *rest) JobTotalCount(ctx context.Context, q types.GetJobsQuery) (int64, error) {
	var count int64
	if err := s.db.WithContext(ctx).Model(&model.Job{}).Where(&model.Job{
		HostName:     q.HostName,
		IDC:          q.IDC,
		Location:     q.Location,
		IP:           q.IP,
		Port:         q.Port,
		DownloadPort: q.DownloadPort,
		JobClusterID: q.JobClusterID,
	}).Count(&count).Error; err != nil {
		return 0, err
	}

	return count, nil
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
