package service

import (
	"d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/manager/types"
)

func (s *service) CreateCDN(json types.CreateCDNRequest) (*model.CDN, error) {
	cdn := &model.CDN{
		Name:   json.Name,
		BIO:    json.BIO,
		Config: json.Config,
	}

	if err := s.db.Create(&cdn).Error; err != nil {
		return nil, err
	}

	return cdn, nil
}

func (s *service) DestroyCDN(id string) error {
	if err := s.db.Unscoped().Delete(&model.CDN{}, id).Error; err != nil {
		return err
	}

	return nil
}

func (s *service) UpdateCDN(id string, json types.UpdateCDNRequest) (*model.CDN, error) {
	cdn := &model.CDN{}
	if err := s.db.First(&cdn, id).Updates(model.CDN{
		Name:   json.Name,
		BIO:    json.BIO,
		Config: json.Config,
	}).Error; err != nil {
		return nil, err
	}

	return cdn, nil
}

func (s *service) GetCDN(id string) (*model.CDN, error) {
	cdn := &model.CDN{}
	if err := s.db.First(&cdn, id).Error; err != nil {
		return nil, err
	}

	return cdn, nil
}

func (s *service) GetCDNs(q types.GetCDNsQuery) (*[]model.CDN, error) {
	cdns := &[]model.CDN{}
	if err := s.db.Scopes(model.Paginate(q.Page, q.PerPage)).Where(&model.CDN{
		Name: q.Name,
	}).Find(&cdns).Error; err != nil {
		return nil, err
	}

	return cdns, nil
}

func (s *service) CDNTotalCount() (int64, error) {
	var count int64
	if err := s.db.Model(&model.CDN{}).Count(&count).Error; err != nil {
		return 0, err
	}

	return count, nil
}
