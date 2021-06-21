package service

import (
	"d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/manager/types"
)

func (s *service) CreateCDN(json types.CreateCDNRequest) (*model.CDN, error) {
	cdn := model.CDN{
		Name:   json.Name,
		BIO:    json.BIO,
		Config: json.Config,
	}

	if err := s.db.Preload("Schedulers").Preload("CDNInstances").Create(&cdn).Error; err != nil {
		return nil, err
	}

	return &cdn, nil
}

func (s *service) DestroyCDN(id uint) error {
	if err := s.db.Unscoped().Delete(&model.CDN{}, id).Error; err != nil {
		return err
	}

	return nil
}

func (s *service) UpdateCDN(id uint, json types.UpdateCDNRequest) (*model.CDN, error) {
	cdn := model.CDN{}
	if err := s.db.Preload("Schedulers").Preload("CDNInstances").First(&cdn, id).Updates(model.CDN{
		Name:   json.Name,
		BIO:    json.BIO,
		Config: json.Config,
	}).Error; err != nil {
		return nil, err
	}

	return &cdn, nil
}

func (s *service) GetCDN(id uint) (*model.CDN, error) {
	cdn := model.CDN{}
	if err := s.db.Preload("Schedulers").Preload("CDNInstances").First(&cdn, id).Error; err != nil {
		return nil, err
	}

	return &cdn, nil
}

func (s *service) GetCDNs(q types.GetCDNsQuery) (*[]model.CDN, error) {
	cdns := []model.CDN{}
	if err := s.db.Preload("Schedulers").Preload("CDNInstances").Scopes(model.Paginate(q.Page, q.PerPage)).Where(&model.CDN{
		Name: q.Name,
	}).Find(&cdns).Error; err != nil {
		return nil, err
	}

	return &cdns, nil
}

func (s *service) CDNTotalCount(q types.GetCDNsQuery) (int64, error) {
	var count int64
	if err := s.db.Preload("Schedulers").Preload("CDNInstances").Model(&model.CDN{}).Where(&model.CDN{
		Name: q.Name,
	}).Count(&count).Error; err != nil {
		return 0, err
	}

	return count, nil
}

func (s *service) AddInstanceToCDN(id, instanceID uint) error {
	cdn := model.CDN{}
	if err := s.db.First(&cdn, id).Error; err != nil {
		return err
	}

	cdnInstance := model.CDNInstance{}
	if err := s.db.First(&cdnInstance, instanceID).Error; err != nil {
		return err
	}

	if err := s.db.Model(&cdn).Association("CDNInstance").Append(&cdnInstance); err != nil {
		return err
	}

	return nil
}

func (s *service) AddSchedulerToCDN(id, schedulerID uint) error {
	cdn := model.CDN{}
	if err := s.db.First(&cdn, id).Error; err != nil {
		return err
	}

	scheduler := model.Scheduler{}
	if err := s.db.First(&scheduler, schedulerID).Error; err != nil {
		return err
	}

	if err := s.db.Model(&cdn).Association("Scheduler").Append(&scheduler); err != nil {
		return err
	}

	return nil
}
