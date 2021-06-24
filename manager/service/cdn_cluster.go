package service

import (
	"d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/manager/types"
)

func (s *service) CreateCDNCluster(json types.CreateCDNClusterRequest) (*model.CDNCluster, error) {
	cdnCluster := model.CDNCluster{
		Name:   json.Name,
		BIO:    json.BIO,
		Config: json.Config,
	}

	if err := s.db.Create(&cdnCluster).Error; err != nil {
		return nil, err
	}

	return &cdnCluster, nil
}

func (s *service) DestroyCDNCluster(id uint) error {
	if err := s.db.Unscoped().Delete(&model.CDNCluster{}, id).Error; err != nil {
		return err
	}

	return nil
}

func (s *service) CreateCDNClusterWithSecurityGroupDomain(json types.CreateCDNClusterRequest) (*model.CDNCluster, error) {
	securityGroup := model.SecurityGroup{
		Domain: json.SecurityGroupDomain,
	}
	if err := s.db.First(&securityGroup).Error; err != nil {
		return s.CreateCDNCluster(json)
	}

	cdnCluster := model.CDNCluster{
		Name:   json.Name,
		BIO:    json.BIO,
		Config: json.Config,
	}

	if err := s.db.Model(&securityGroup).Association("CDNClusters").Append(&cdnCluster); err != nil {
		return nil, err

	}

	return &cdnCluster, nil
}

func (s *service) UpdateCDNCluster(id uint, json types.UpdateCDNClusterRequest) (*model.CDNCluster, error) {
	cdnCluster := model.CDNCluster{}
	if err := s.db.First(&cdnCluster, id).Updates(model.CDNCluster{
		Name:   json.Name,
		BIO:    json.BIO,
		Config: json.Config,
	}).Error; err != nil {
		return nil, err
	}

	return &cdnCluster, nil
}

func (s *service) UpdateCDNClusterWithSecurityGroupDomain(id uint, json types.UpdateCDNClusterRequest) (*model.CDNCluster, error) {
	securityGroup := model.SecurityGroup{
		Domain: json.SecurityGroupDomain,
	}
	if err := s.db.First(&securityGroup).Error; err != nil {
		return s.UpdateCDNCluster(id, json)
	}

	cdnCluster := model.CDNCluster{
		Name:   json.Name,
		BIO:    json.BIO,
		Config: json.Config,
	}

	if err := s.db.Model(&securityGroup).Association("CDNClusters").Append(&cdnCluster); err != nil {
		return nil, err
	}

	return &cdnCluster, nil
}

func (s *service) GetCDNCluster(id uint) (*model.CDNCluster, error) {
	cdnCluster := model.CDNCluster{}
	if err := s.db.First(&cdnCluster, id).Error; err != nil {
		return nil, err
	}

	return &cdnCluster, nil
}

func (s *service) GetCDNClusters(q types.GetCDNClustersQuery) (*[]model.CDNCluster, error) {
	cdnClusters := []model.CDNCluster{}
	if err := s.db.Scopes(model.Paginate(q.Page, q.PerPage)).Where(&model.CDNCluster{
		Name: q.Name,
	}).Find(&cdnClusters).Error; err != nil {
		return nil, err
	}

	return &cdnClusters, nil
}

func (s *service) CDNClusterTotalCount(q types.GetCDNClustersQuery) (int64, error) {
	var count int64
	if err := s.db.Model(&model.CDNCluster{}).Where(&model.CDNCluster{
		Name: q.Name,
	}).Count(&count).Error; err != nil {
		return 0, err
	}

	return count, nil
}

func (s *service) AddCDNToCDNCluster(id, cdnID uint) error {
	cdnCluster := model.CDNCluster{}
	if err := s.db.First(&cdnCluster, id).Error; err != nil {
		return err
	}

	cdn := model.CDN{}
	if err := s.db.First(&cdn, cdnID).Error; err != nil {
		return err
	}

	if err := s.db.Model(&cdnCluster).Association("CDNs").Append(&cdn); err != nil {
		return err
	}

	return nil
}

func (s *service) AddSchedulerClusterToCDNCluster(id, schedulerClusterID uint) error {
	cdnCluster := model.CDNCluster{}
	if err := s.db.First(&cdnCluster, id).Error; err != nil {
		return err
	}

	SchedulerCluster := model.SchedulerCluster{}
	if err := s.db.First(&SchedulerCluster, schedulerClusterID).Error; err != nil {
		return err
	}

	if err := s.db.Model(&cdnCluster).Association("SchedulerClusters").Append(&SchedulerCluster); err != nil {
		return err
	}

	return nil
}
