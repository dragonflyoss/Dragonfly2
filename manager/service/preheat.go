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
			SchedulerClusterID: &schedulerCluster.ID,
			Status:             model.SchedulerStatusActive,
		}).Error; err != nil {
			return nil, err
		}

		return s.tasks.CreatePreheat([]string{scheduler.HostName}, json)
	}

	schedulerClusters := []model.SchedulerCluster{}
	if err := s.db.Find(&schedulerClusters).Error; err != nil {
		return nil, err
	}

	hostnames := []string{}
	for _, schedulerCluster := range schedulerClusters {
		scheduler := model.Scheduler{}
		if err := s.db.First(&scheduler, model.Scheduler{
			SchedulerClusterID: &schedulerCluster.ID,
			Status:             model.SchedulerStatusActive,
		}).Error; err != nil {
			continue
		}

		hostnames = append(hostnames, scheduler.HostName)
	}

	return s.tasks.CreatePreheat(hostnames, json)
}

func (s *rest) GetPreheat(id string) (*types.Preheat, error) {
	return s.tasks.GetPreheat(id)
}
