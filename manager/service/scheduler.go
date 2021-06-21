package service

import (
	"d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/manager/types"
)

func (s *service) CreateScheduler(json types.CreateSchedulerRequest) (*model.Scheduler, error) {
	scheduler := model.Scheduler{
		Name:         json.Name,
		BIO:          json.BIO,
		Config:       json.Config,
		ClientConfig: json.ClientConfig,
	}

	if err := s.db.Preload("CDNs").Preload("SchedulerInstances").Create(&scheduler).Error; err != nil {
		return nil, err
	}

	return &scheduler, nil
}

func (s *service) DestroyScheduler(id uint) error {
	if err := s.db.Unscoped().Delete(&model.Scheduler{}, id).Error; err != nil {
		return err
	}

	return nil
}

func (s *service) UpdateScheduler(id uint, json types.UpdateSchedulerRequest) (*model.Scheduler, error) {
	scheduler := model.Scheduler{}
	if err := s.db.Preload("CDNs").Preload("SchedulerInstances").First(&scheduler, id).Updates(model.Scheduler{
		Name:         json.Name,
		BIO:          json.BIO,
		Config:       json.Config,
		ClientConfig: json.ClientConfig,
	}).Error; err != nil {
		return nil, err
	}

	return &scheduler, nil
}

func (s *service) GetScheduler(id uint) (*model.Scheduler, error) {
	scheduler := model.Scheduler{}
	if err := s.db.Preload("CDNs").Preload("SchedulerInstances").First(&scheduler, id).Error; err != nil {
		return nil, err
	}

	return &scheduler, nil
}

func (s *service) GetSchedulers(q types.GetSchedulersQuery) (*[]model.Scheduler, error) {
	schedulers := []model.Scheduler{}
	if err := s.db.Preload("CDNs").Preload("SchedulerInstances").Scopes(model.Paginate(q.Page, q.PerPage)).Where(&model.Scheduler{
		Name: q.Name,
	}).Find(&schedulers).Error; err != nil {
		return nil, err
	}

	return &schedulers, nil
}

func (s *service) SchedulerTotalCount(q types.GetSchedulersQuery) (int64, error) {
	var count int64
	if err := s.db.Model(&model.Scheduler{}).Where(&model.Scheduler{
		Name: q.Name,
	}).Count(&count).Error; err != nil {
		return 0, err
	}

	return count, nil
}

func (s *service) AddInstanceToScheduler(id, instanceID uint) error {
	scheduler := model.Scheduler{}
	if err := s.db.First(&scheduler, id).Error; err != nil {
		return err
	}

	schedulerInstance := model.SchedulerInstance{}
	if err := s.db.First(&schedulerInstance, instanceID).Error; err != nil {
		return err
	}

	if err := s.db.Model(&scheduler).Association("SchedulerInstances").Append(&schedulerInstance); err != nil {
		return err
	}

	return nil
}
