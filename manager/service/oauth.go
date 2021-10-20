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
	"context"

	"d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/manager/types"
)

func (s *rest) CreateOauth(ctx context.Context, json types.CreateOauthRequest) (*model.Oauth, error) {
	oauth := model.Oauth{
		Name:         json.Name,
		BIO:          json.BIO,
		ClientID:     json.ClientID,
		ClientSecret: json.ClientSecret,
		RedirectURL:  json.RedirectURL,
	}

	if err := s.db.Create(&oauth).Error; err != nil {
		return nil, err
	}

	return &oauth, nil
}

func (s *rest) DestroyOauth(ctx context.Context, id uint) error {
	oauth := model.Oauth{}
	if err := s.db.First(&oauth, id).Error; err != nil {
		return err
	}

	if err := s.db.Unscoped().Delete(&model.Oauth{}, id).Error; err != nil {
		return err
	}

	return nil
}

func (s *rest) UpdateOauth(ctx context.Context, id uint, json types.UpdateOauthRequest) (*model.Oauth, error) {
	oauth := model.Oauth{}
	if err := s.db.First(&oauth, id).Updates(model.Oauth{
		Name:         json.Name,
		BIO:          json.BIO,
		ClientID:     json.ClientID,
		ClientSecret: json.ClientSecret,
		RedirectURL:  json.RedirectURL,
	}).Error; err != nil {
		return nil, err
	}

	return &oauth, nil
}

func (s *rest) GetOauth(ctx context.Context, id uint) (*model.Oauth, error) {
	oauth := model.Oauth{}
	if err := s.db.First(&oauth, id).Error; err != nil {
		return nil, err
	}

	return &oauth, nil
}

func (s *rest) GetOauths(ctx context.Context, q types.GetOauthsQuery) (*[]model.Oauth, error) {
	oauths := []model.Oauth{}
	if err := s.db.Scopes(model.Paginate(q.Page, q.PerPage)).Where(&model.Oauth{
		Name:     q.Name,
		ClientID: q.ClientID,
	}).Find(&oauths).Error; err != nil {
		return nil, err
	}

	return &oauths, nil
}

func (s *rest) OauthTotalCount(ctx context.Context, q types.GetOauthsQuery) (int64, error) {
	var count int64
	if err := s.db.Model(&model.Oauth{}).Where(&model.Oauth{
		Name:     q.Name,
		ClientID: q.ClientID,
	}).Count(&count).Error; err != nil {
		return 0, err
	}

	return count, nil
}
