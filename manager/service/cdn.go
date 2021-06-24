package service

import (
	"d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/manager/types"
)

func (s *service) CreateCDN(json types.CreateCDNRequest) (*model.CDN, error) {
	cdn := model.CDN{
		HostName:     json.HostName,
		IDC:          json.IDC,
		Location:     json.Location,
		IP:           json.IP,
		Port:         json.Port,
		DownloadPort: json.DownloadPort,
	}

	if err := s.db.Create(&cdn).Error; err != nil {
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
	if err := s.db.First(&cdn, id).Updates(model.CDN{
		IDC:          json.IDC,
		Location:     json.Location,
		IP:           json.IP,
		Port:         json.Port,
		DownloadPort: json.DownloadPort,
	}).Error; err != nil {
		return nil, err
	}

	return &cdn, nil
}

func (s *service) GetCDN(id uint) (*model.CDN, error) {
	cdn := model.CDN{}
	if err := s.db.First(&cdn, id).Error; err != nil {
		return nil, err
	}

	return &cdn, nil
}

func (s *service) GetCDNs(q types.GetCDNsQuery) (*[]model.CDN, error) {
	cdns := []model.CDN{}
	if err := s.db.Scopes(model.Paginate(q.Page, q.PerPage)).Where(&model.CDN{
		HostName:     q.HostName,
		IDC:          q.IDC,
		Location:     q.Location,
		IP:           q.IP,
		Port:         q.Port,
		DownloadPort: q.DownloadPort,
	}).Find(&cdns).Error; err != nil {
		return nil, err
	}

	return &cdns, nil
}

func (s *service) CDNTotalCount(q types.GetCDNsQuery) (int64, error) {
	var count int64
	if err := s.db.Model(&model.CDN{}).Where(&model.CDN{
		HostName:     q.HostName,
		IDC:          q.IDC,
		Location:     q.Location,
		IP:           q.IP,
		Port:         q.Port,
		DownloadPort: q.DownloadPort,
	}).Count(&count).Error; err != nil {
		return 0, err
	}

	return count, nil
}
