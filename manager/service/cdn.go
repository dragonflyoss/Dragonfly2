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
