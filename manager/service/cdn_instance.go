package service

import (
	"d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/manager/types"
)

func (s *service) CreateCDNInstance(json types.CreateCDNInstanceRequest) (*model.CDNInstance, error) {
	cdnInstance := model.CDNInstance{
		Host:         json.Host,
		IDC:          json.IDC,
		Location:     json.Location,
		IP:           json.IP,
		Port:         json.Port,
		DownloadPort: json.DownloadPort,
	}

	if err := s.db.Preload("CDN").Preload("SecurityGroup").Create(&cdnInstance).Error; err != nil {
		return nil, err
	}

	return &cdnInstance, nil
}

func (s *service) CreateCDNInstanceWithSecurityGroupDomain(json types.CreateCDNInstanceRequest) (*model.CDNInstance, error) {
	securityGroup := model.SecurityGroup{
		Domain: json.SecurityGroupDomain,
	}
	if err := s.db.Preload("CDN").Preload("SecurityGroup").First(&securityGroup).Error; err != nil {
		return s.CreateCDNInstance(json)
	}

	cdnInstance := model.CDNInstance{
		Host:         json.Host,
		IDC:          json.IDC,
		Location:     json.Location,
		IP:           json.IP,
		Port:         json.Port,
		DownloadPort: json.DownloadPort,
	}

	if err := s.db.Model(&securityGroup).Association("CDNInstance").Append(&cdnInstance); err != nil {
		return nil, err

	}

	return s.GetCDNInstance(cdnInstance.ID)
}

func (s *service) DestroyCDNInstance(id uint) error {
	if err := s.db.Unscoped().Delete(&model.CDNInstance{}, id).Error; err != nil {
		return err
	}

	return nil
}

func (s *service) UpdateCDNInstance(id uint, json types.UpdateCDNInstanceRequest) (*model.CDNInstance, error) {
	cdnInstance := model.CDNInstance{}
	if err := s.db.Preload("CDN").Preload("SecurityGroup").First(&cdnInstance, id).Updates(model.CDNInstance{
		IDC:          json.IDC,
		Location:     json.Location,
		IP:           json.IP,
		Port:         json.Port,
		DownloadPort: json.DownloadPort,
	}).Error; err != nil {
		return nil, err
	}

	return &cdnInstance, nil
}

func (s *service) UpdateCDNInstanceWithSecurityGroupDomain(id uint, json types.UpdateCDNInstanceRequest) (*model.CDNInstance, error) {
	securityGroup := model.SecurityGroup{
		Domain: json.SecurityGroupDomain,
	}
	if err := s.db.First(&securityGroup).Error; err != nil {
		return s.UpdateCDNInstance(id, json)
	}

	cdnInstance := model.CDNInstance{
		IDC:          json.IDC,
		Location:     json.Location,
		IP:           json.IP,
		Port:         json.Port,
		DownloadPort: json.DownloadPort,
	}

	if err := s.db.Model(&securityGroup).Association("CDNInstance").Append(&cdnInstance); err != nil {
		return nil, err
	}

	return s.GetCDNInstance(cdnInstance.ID)
}

func (s *service) GetCDNInstance(id uint) (*model.CDNInstance, error) {
	cdnInstance := model.CDNInstance{}
	if err := s.db.Preload("CDN").Preload("SecurityGroup").First(&cdnInstance, id).Error; err != nil {
		return nil, err
	}

	return &cdnInstance, nil
}

func (s *service) GetCDNInstances(q types.GetCDNInstancesQuery) (*[]model.CDNInstance, error) {
	cdnInstances := []model.CDNInstance{}
	if err := s.db.Preload("CDN").Preload("SecurityGroup").Scopes(model.Paginate(q.Page, q.PerPage)).Where(&model.CDNInstance{
		Host:         q.Host,
		IDC:          q.IDC,
		Location:     q.Location,
		IP:           q.IP,
		Port:         q.Port,
		DownloadPort: q.DownloadPort,
	}).Find(&cdnInstances).Error; err != nil {
		return nil, err
	}

	return &cdnInstances, nil
}

func (s *service) CDNInstanceTotalCount(q types.GetCDNInstancesQuery) (int64, error) {
	var count int64
	if err := s.db.Model(&model.CDNInstance{}).Where(&model.CDNInstance{
		Host:         q.Host,
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
