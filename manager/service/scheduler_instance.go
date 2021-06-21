package service

import (
	"d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/manager/types"
)

func (s *service) CreateSchedulerInstance(json types.CreateSchedulerInstanceRequest) (*model.SchedulerInstance, error) {
	schedulerInstance := model.SchedulerInstance{
		Host:      json.Host,
		VIPs:      json.VIPs,
		IDC:       json.IDC,
		Location:  json.Location,
		NetConfig: json.NetConfig,
		IP:        json.IP,
		Port:      json.Port,
	}

	if err := s.db.Preload("Scheduler").Preload("SecurityGroup").Create(&schedulerInstance).Error; err != nil {
		return nil, err
	}

	return &schedulerInstance, nil
}

func (s *service) CreateSchedulerInstanceWithSecurityGroupDomain(json types.CreateSchedulerInstanceRequest) (*model.SchedulerInstance, error) {
	securityGroup := model.SecurityGroup{
		Domain: json.SecurityGroupDomain,
	}
	if err := s.db.First(&securityGroup).Error; err != nil {
		return s.CreateSchedulerInstance(json)
	}

	schedulerInstance := model.SchedulerInstance{
		Host:      json.Host,
		VIPs:      json.VIPs,
		IDC:       json.IDC,
		Location:  json.Location,
		NetConfig: json.NetConfig,
		IP:        json.IP,
		Port:      json.Port,
	}

	if err := s.db.Model(&securityGroup).Association("SchedulerInstances").Append(&schedulerInstance); err != nil {
		return nil, err
	}

	return s.GetSchedulerInstance(schedulerInstance.ID)
}

func (s *service) DestroySchedulerInstance(id uint) error {
	if err := s.db.Unscoped().Delete(&model.SchedulerInstance{}, id).Error; err != nil {
		return err
	}

	return nil
}

func (s *service) UpdateSchedulerInstance(id uint, json types.UpdateSchedulerInstanceRequest) (*model.SchedulerInstance, error) {
	schedulerInstance := model.SchedulerInstance{}
	if err := s.db.Preload("Scheduler").Preload("SecurityGroup").First(&schedulerInstance, id).Updates(model.SchedulerInstance{
		Host:      json.Host,
		VIPs:      json.VIPs,
		IDC:       json.IDC,
		Location:  json.Location,
		NetConfig: json.NetConfig,
		IP:        json.IP,
		Port:      json.Port,
	}).Error; err != nil {
		return nil, err
	}

	return &schedulerInstance, nil
}

func (s *service) UpdateSchedulerInstanceWithSecurityGroupDomain(id uint, json types.UpdateSchedulerInstanceRequest) (*model.SchedulerInstance, error) {
	securityGroup := model.SecurityGroup{
		Domain: json.SecurityGroupDomain,
	}
	if err := s.db.First(&securityGroup).Error; err != nil {
		return s.UpdateSchedulerInstance(id, json)
	}

	schedulerInstance := model.SchedulerInstance{
		Host:      json.Host,
		VIPs:      json.VIPs,
		IDC:       json.IDC,
		Location:  json.Location,
		NetConfig: json.NetConfig,
		IP:        json.IP,
		Port:      json.Port,
	}

	if err := s.db.Model(&securityGroup).Association("SchedulerInstances").Append(&schedulerInstance); err != nil {
		return nil, err
	}

	return s.GetSchedulerInstance(schedulerInstance.ID)
}

func (s *service) GetSchedulerInstance(id uint) (*model.SchedulerInstance, error) {
	schedulerInstance := model.SchedulerInstance{}
	if err := s.db.Preload("Scheduler").Preload("SecurityGroup").First(&schedulerInstance, id).Error; err != nil {
		return nil, err
	}

	return &schedulerInstance, nil
}

func (s *service) GetSchedulerInstances(q types.GetSchedulerInstancesQuery) (*[]model.SchedulerInstance, error) {
	schedulerInstances := []model.SchedulerInstance{}
	if err := s.db.Preload("Scheduler").Preload("SecurityGroup").Scopes(model.Paginate(q.Page, q.PerPage)).Where(&model.SchedulerInstance{
		Host:     q.Host,
		IDC:      q.IDC,
		Location: q.Location,
		IP:       q.IP,
		Status:   q.Status,
	}).Find(&schedulerInstances).Error; err != nil {
		return nil, err
	}

	return &schedulerInstances, nil
}

func (s *service) SchedulerInstanceTotalCount(q types.GetSchedulerInstancesQuery) (int64, error) {
	var count int64
	if err := s.db.Model(&model.SchedulerInstance{}).Where(&model.SchedulerInstance{
		Host:     q.Host,
		IDC:      q.IDC,
		Location: q.Location,
		IP:       q.IP,
		Status:   q.Status,
	}).Count(&count).Error; err != nil {
		return 0, err
	}

	return count, nil
}
