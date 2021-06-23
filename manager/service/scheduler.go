package service

import (
	"d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/manager/types"
)

func (s *service) CreateScheduler(json types.CreateSchedulerRequest) (*model.Scheduler, error) {
	scheduler := model.Scheduler{
		HostName:  json.HostName,
		VIPs:      json.VIPs,
		IDC:       json.IDC,
		Location:  json.Location,
		NetConfig: json.NetConfig,
		IP:        json.IP,
		Port:      json.Port,
	}

	if err := s.db.Create(&scheduler).Error; err != nil {
		return nil, err
	}

	return &scheduler, nil
}

func (s *service) CreateSchedulerWithSecurityGroupDomain(json types.CreateSchedulerRequest) (*model.Scheduler, error) {
	securityGroup := model.SecurityGroup{
		Domain: json.SecurityGroupDomain,
	}
	if err := s.db.First(&securityGroup).Error; err != nil {
		return s.CreateScheduler(json)
	}

	scheduler := model.Scheduler{
		HostName:  json.HostName,
		VIPs:      json.VIPs,
		IDC:       json.IDC,
		Location:  json.Location,
		NetConfig: json.NetConfig,
		IP:        json.IP,
		Port:      json.Port,
	}

	if err := s.db.Model(&securityGroup).Association("Schedulers").Append(&scheduler); err != nil {
		return nil, err
	}

	return s.GetScheduler(scheduler.ID)
}

func (s *service) DestroyScheduler(id uint) error {
	if err := s.db.Unscoped().Delete(&model.Scheduler{}, id).Error; err != nil {
		return err
	}

	return nil
}

func (s *service) UpdateScheduler(id uint, json types.UpdateSchedulerRequest) (*model.Scheduler, error) {
	scheduler := model.Scheduler{}
	if err := s.db.First(&scheduler, id).Updates(model.Scheduler{
		VIPs:      json.VIPs,
		IDC:       json.IDC,
		Location:  json.Location,
		NetConfig: json.NetConfig,
		IP:        json.IP,
		Port:      json.Port,
	}).Error; err != nil {
		return nil, err
	}

	return &scheduler, nil
}

func (s *service) UpdateSchedulerWithSecurityGroupDomain(id uint, json types.UpdateSchedulerRequest) (*model.Scheduler, error) {
	securityGroup := model.SecurityGroup{
		Domain: json.SecurityGroupDomain,
	}
	if err := s.db.First(&securityGroup).Error; err != nil {
		return s.UpdateScheduler(id, json)
	}

	scheduler := model.Scheduler{
		VIPs:      json.VIPs,
		IDC:       json.IDC,
		Location:  json.Location,
		NetConfig: json.NetConfig,
		IP:        json.IP,
		Port:      json.Port,
	}

	if err := s.db.Model(&securityGroup).Association("Schedulers").Append(&scheduler); err != nil {
		return nil, err
	}

	return s.GetScheduler(scheduler.ID)
}

func (s *service) GetScheduler(id uint) (*model.Scheduler, error) {
	scheduler := model.Scheduler{}
	if err := s.db.First(&scheduler, id).Error; err != nil {
		return nil, err
	}

	return &scheduler, nil
}

func (s *service) GetSchedulers(q types.GetSchedulersQuery) (*[]model.Scheduler, error) {
	schedulers := []model.Scheduler{}
	if err := s.db.Scopes(model.Paginate(q.Page, q.PerPage)).Where(&model.Scheduler{
		HostName: q.HostName,
		IDC:      q.IDC,
		Location: q.Location,
		IP:       q.IP,
		Status:   q.Status,
	}).Find(&schedulers).Error; err != nil {
		return nil, err
	}

	return &schedulers, nil
}

func (s *service) SchedulerTotalCount(q types.GetSchedulersQuery) (int64, error) {
	var count int64
	if err := s.db.Model(&model.Scheduler{}).Where(&model.Scheduler{
		HostName: q.HostName,
		IDC:      q.IDC,
		Location: q.Location,
		IP:       q.IP,
		Status:   q.Status,
	}).Count(&count).Error; err != nil {
		return 0, err
	}

	return count, nil
}
