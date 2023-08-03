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

	"d7y.io/dragonfly/v2/manager/models"
	"d7y.io/dragonfly/v2/manager/types"
)

func (s *service) CreateOauth(ctx context.Context, json types.CreateOauthRequest) (*models.Oauth, error) {
	oauth := models.Oauth{
		Name:         json.Name,
		BIO:          json.BIO,
		ClientID:     json.ClientID,
		ClientSecret: json.ClientSecret,
		RedirectURL:  json.RedirectURL,
	}

	if err := s.db.WithContext(ctx).Create(&oauth).Error; err != nil {
		return nil, err
	}

	return &oauth, nil
}

func (s *service) DestroyOauth(ctx context.Context, id uint) error {
	oauth := models.Oauth{}
	if err := s.db.WithContext(ctx).First(&oauth, id).Error; err != nil {
		return err
	}

	if err := s.db.WithContext(ctx).Unscoped().Unscoped().Delete(&models.Oauth{}, id).Error; err != nil {
		return err
	}

	return nil
}

func (s *service) UpdateOauth(ctx context.Context, id uint, json types.UpdateOauthRequest) (*models.Oauth, error) {
	oauth := models.Oauth{}
	if err := s.db.WithContext(ctx).First(&oauth, id).Updates(models.Oauth{
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

func (s *service) GetOauth(ctx context.Context, id uint) (*models.Oauth, error) {
	oauth := models.Oauth{}
	if err := s.db.WithContext(ctx).First(&oauth, id).Error; err != nil {
		return nil, err
	}

	return &oauth, nil
}

func (s *service) GetOauths(ctx context.Context, q types.GetOauthsQuery) ([]models.Oauth, int64, error) {
	var count int64
	var oauths []models.Oauth
	if err := s.db.WithContext(ctx).Scopes(models.Paginate(q.Page, q.PerPage)).Where(&models.Oauth{
		Name:     q.Name,
		ClientID: q.ClientID,
	}).Find(&oauths).Limit(-1).Offset(-1).Count(&count).Error; err != nil {
		return nil, 0, err
	}

	return oauths, count, nil
}
