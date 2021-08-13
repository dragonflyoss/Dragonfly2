package service

import (
	"d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/manager/types"
)

func (s *rest) CreateSetting(json types.CreateSettingRequest) (*model.Settings, error) {
	setting := model.Settings{
		Key:   json.Key,
		Value: json.Value,
	}

	if err := s.db.Create(&setting).Error; err != nil {
		return nil, err
	}

	return &setting, nil
}

func (s *rest) DestroySetting(key string) error {
	if err := s.db.Unscoped().Delete(&model.Settings{}, model.Settings{Key: key}).Error; err != nil {
		return err
	}

	return nil
}

func (s *rest) UpdateSetting(key string, json types.UpdateSettingRequest) (*model.Settings, error) {
	setting := model.Settings{}
	if err := s.db.First(&setting, model.Settings{Key: key}).Updates(model.Settings{
		Key:   json.Key,
		Value: json.Value,
	}).Error; err != nil {
		return nil, err
	}

	return &setting, nil
}

func (s *rest) GetSettings(q types.GetSettingsQuery) (*[]model.Settings, error) {
	settings := []model.Settings{}
	if err := s.db.Scopes(model.Paginate(q.Page, q.PerPage)).Find(&settings).Error; err != nil {
		return nil, err
	}

	return &settings, nil
}

func (s *rest) SettingTotalCount() (int64, error) {
	var count int64
	if err := s.db.Model(&model.Settings{}).Count(&count).Error; err != nil {
		return 0, err
	}

	return count, nil
}
