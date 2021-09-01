/*
 *     Copyright 2020 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package service

import (
	"d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/manager/types"
)

func (s *rest) CreateOauth2(json types.CreateOauth2Request) (*model.Oauth2, error) {
	oauth2 := model.Oauth2{
		Name:         json.Name,
		BIO:          json.BIO,
		ClientID:     json.ClientID,
		ClientSecret: json.ClientSecret,
	}

	if err := s.db.Create(&oauth2).Error; err != nil {
		return nil, err
	}

	return &oauth2, nil
}

func (s *rest) DestroyOauth2(id uint) error {
	if err := s.db.Unscoped().Delete(&model.Oauth2{}, id).Error; err != nil {
		return err
	}

	return nil
}

func (s *rest) UpdateOauth2(id uint, json types.UpdateOauth2Request) (*model.Oauth2, error) {
	oauth2 := model.Oauth2{}
	if err := s.db.First(&oauth2, id).Updates(model.Oauth2{
		Name:         json.Name,
		BIO:          json.BIO,
		ClientID:     json.ClientID,
		ClientSecret: json.ClientSecret,
	}).Error; err != nil {
		return nil, err
	}

	return &oauth2, nil
}

func (s *rest) GetOauth2(id uint) (*model.Oauth2, error) {
	oauth2 := model.Oauth2{}
	if err := s.db.First(&oauth2, id).Error; err != nil {
		return nil, err
	}

	return &oauth2, nil
}

func (s *rest) GetOauth2s(q types.GetOauth2sQuery) (*[]model.Oauth2, error) {
	oauth2s := []model.Oauth2{}
	if err := s.db.Scopes(model.Paginate(q.Page, q.PerPage)).Where(&model.Oauth2{
		Name:     q.Name,
		ClientID: q.ClientID,
	}).Find(&oauth2s).Error; err != nil {
		return nil, err
	}

	return &oauth2s, nil
}

func (s *rest) Oauth2TotalCount(q types.GetOauth2sQuery) (int64, error) {
	var count int64
	if err := s.db.Model(&model.Oauth2{}).Where(&model.Oauth2{
		Name:     q.Name,
		ClientID: q.ClientID,
	}).Count(&count).Error; err != nil {
		return 0, err
	}

	return count, nil
}
