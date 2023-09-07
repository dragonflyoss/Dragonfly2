/*
 *     Copyright 2023 The Dragonfly Authors
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
	"encoding/base64"

	"github.com/google/uuid"

	"d7y.io/dragonfly/v2/manager/models"
	"d7y.io/dragonfly/v2/manager/types"
)

func (s *service) CreatePersonalAccessToken(ctx context.Context, json types.CreatePersonalAccessTokenRequest) (*models.PersonalAccessToken, error) {
	personalAccessToken := models.PersonalAccessToken{
		Name:      json.Name,
		BIO:       json.BIO,
		Token:     s.generatePersonalAccessToken(),
		Scopes:    json.Scopes,
		State:     models.PersonalAccessTokenStateActive,
		ExpiredAt: json.ExpiredAt,
		UserID:    json.UserID,
	}

	if err := s.db.WithContext(ctx).Create(&personalAccessToken).Error; err != nil {
		return nil, err
	}

	return &personalAccessToken, nil
}

func (s *service) DestroyPersonalAccessToken(ctx context.Context, id uint) error {
	personalAccessToken := models.PersonalAccessToken{}
	if err := s.db.WithContext(ctx).First(&personalAccessToken, id).Error; err != nil {
		return err
	}

	if err := s.db.WithContext(ctx).Unscoped().Delete(&models.PersonalAccessToken{}, id).Error; err != nil {
		return err
	}

	return nil
}

func (s *service) UpdatePersonalAccessToken(ctx context.Context, id uint, json types.UpdatePersonalAccessTokenRequest) (*models.PersonalAccessToken, error) {
	personalAccessToken := models.PersonalAccessToken{}
	if err := s.db.WithContext(ctx).Preload("User").First(&personalAccessToken, id).Updates(models.PersonalAccessToken{
		BIO:       json.BIO,
		Scopes:    json.Scopes,
		State:     json.State,
		ExpiredAt: json.ExpiredAt,
		UserID:    json.UserID,
	}).Error; err != nil {
		return nil, err
	}

	return &personalAccessToken, nil
}

func (s *service) GetPersonalAccessToken(ctx context.Context, id uint) (*models.PersonalAccessToken, error) {
	personalAccessToken := models.PersonalAccessToken{}
	if err := s.db.WithContext(ctx).Preload("User").First(&personalAccessToken, id).Error; err != nil {
		return nil, err
	}

	return &personalAccessToken, nil
}

func (s *service) GetPersonalAccessTokens(ctx context.Context, q types.GetPersonalAccessTokensQuery) ([]models.PersonalAccessToken, int64, error) {
	var count int64
	personalAccessToken := []models.PersonalAccessToken{}
	if err := s.db.WithContext(ctx).Scopes(models.Paginate(q.Page, q.PerPage)).Where(&models.PersonalAccessToken{
		State:  q.State,
		UserID: q.UserID,
	}).Preload("User").Find(&personalAccessToken).Limit(-1).Offset(-1).Count(&count).Error; err != nil {
		return nil, 0, err
	}

	return personalAccessToken, count, nil
}

func (s *service) generatePersonalAccessToken() string {
	return base64.RawURLEncoding.EncodeToString([]byte(uuid.NewString()))
}
