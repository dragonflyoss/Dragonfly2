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

func (s *service) CreateCDN(ctx context.Context, json types.CreateCDNRequest) (*model.CDN, error) {
	cdn := model.CDN{
		HostName:     json.HostName,
		IDC:          json.IDC,
		Location:     json.Location,
		IP:           json.IP,
		Port:         json.Port,
		DownloadPort: json.DownloadPort,
		CDNClusterID: json.CDNClusterID,
	}

	if err := s.db.WithContext(ctx).Create(&cdn).Error; err != nil {
		return nil, err
	}

	return &cdn, nil
}

func (s *service) DestroyCDN(ctx context.Context, id uint) error {
	cdn := model.CDN{}
	if err := s.db.WithContext(ctx).First(&cdn, id).Error; err != nil {
		return err
	}

	if err := s.db.WithContext(ctx).Unscoped().Delete(&model.CDN{}, id).Error; err != nil {
		return err
	}

	return nil
}

func (s *service) UpdateCDN(ctx context.Context, id uint, json types.UpdateCDNRequest) (*model.CDN, error) {
	cdn := model.CDN{}
	if err := s.db.WithContext(ctx).First(&cdn, id).Updates(model.CDN{
		IDC:          json.IDC,
		Location:     json.Location,
		IP:           json.IP,
		Port:         json.Port,
		DownloadPort: json.DownloadPort,
		CDNClusterID: json.CDNClusterID,
	}).Error; err != nil {
		return nil, err
	}

	return &cdn, nil
}

func (s *service) GetCDN(ctx context.Context, id uint) (*model.CDN, error) {
	cdn := model.CDN{}
	if err := s.db.WithContext(ctx).First(&cdn, id).Error; err != nil {
		return nil, err
	}

	return &cdn, nil
}

func (s *service) GetCDNs(ctx context.Context, q types.GetCDNsQuery) (*[]model.CDN, int64, error) {
	var count int64
	var cdns []model.CDN
	if err := s.db.WithContext(ctx).Scopes(model.Paginate(q.Page, q.PerPage)).Where(&model.CDN{
		HostName:     q.HostName,
		IDC:          q.IDC,
		Location:     q.Location,
		IP:           q.IP,
		Port:         q.Port,
		DownloadPort: q.DownloadPort,
		CDNClusterID: q.CDNClusterID,
	}).Find(&cdns).Count(&count).Error; err != nil {
		return nil, 0, err
	}

	return &cdns, count, nil
}
