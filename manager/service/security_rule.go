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

func (s *service) CreateSecurityRule(ctx context.Context, json types.CreateSecurityRuleRequest) (*models.SecurityRule, error) {
	securityRule := models.SecurityRule{
		Name:        json.Name,
		BIO:         json.BIO,
		Domain:      json.Domain,
		ProxyDomain: json.ProxyDomain,
	}

	if err := s.db.WithContext(ctx).Create(&securityRule).Error; err != nil {
		return nil, err
	}

	return &securityRule, nil
}

func (s *service) DestroySecurityRule(ctx context.Context, id uint) error {
	securityRule := models.SecurityRule{}
	if err := s.db.WithContext(ctx).First(&securityRule, id).Error; err != nil {
		return err
	}

	if err := s.db.WithContext(ctx).Unscoped().Delete(&models.SecurityRule{}, id).Error; err != nil {
		return err
	}

	return nil
}

func (s *service) UpdateSecurityRule(ctx context.Context, id uint, json types.UpdateSecurityRuleRequest) (*models.SecurityRule, error) {
	securityRule := models.SecurityRule{}
	if err := s.db.WithContext(ctx).First(&securityRule, id).Updates(models.SecurityRule{
		Name:        json.Name,
		BIO:         json.BIO,
		Domain:      json.Domain,
		ProxyDomain: json.ProxyDomain,
	}).Error; err != nil {
		return nil, err
	}

	return &securityRule, nil
}

func (s *service) GetSecurityRule(ctx context.Context, id uint) (*models.SecurityRule, error) {
	securityRule := models.SecurityRule{}
	if err := s.db.WithContext(ctx).First(&securityRule, id).Error; err != nil {
		return nil, err
	}

	return &securityRule, nil
}

func (s *service) GetSecurityRules(ctx context.Context, q types.GetSecurityRulesQuery) ([]models.SecurityRule, int64, error) {
	var count int64
	var securityRules []models.SecurityRule
	if err := s.db.WithContext(ctx).Scopes(models.Paginate(q.Page, q.PerPage)).Where(&models.SecurityRule{
		Name: q.Name,
	}).Find(&securityRules).Limit(-1).Offset(-1).Count(&count).Error; err != nil {
		return nil, 0, err
	}

	return securityRules, count, nil
}
