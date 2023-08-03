/*
 *     Copyright 2022 The Dragonfly Authors
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

func (s *service) CreateSeedPeer(ctx context.Context, json types.CreateSeedPeerRequest) (*models.SeedPeer, error) {
	seedPeer := models.SeedPeer{
		Hostname:          json.Hostname,
		Type:              json.Type,
		IDC:               json.IDC,
		Location:          json.Location,
		IP:                json.IP,
		Port:              json.Port,
		DownloadPort:      json.DownloadPort,
		ObjectStoragePort: json.ObjectStoragePort,
		SeedPeerClusterID: json.SeedPeerClusterID,
	}

	if err := s.db.WithContext(ctx).Create(&seedPeer).Error; err != nil {
		return nil, err
	}

	return &seedPeer, nil
}

func (s *service) DestroySeedPeer(ctx context.Context, id uint) error {
	seedPeer := models.SeedPeer{}
	if err := s.db.WithContext(ctx).First(&seedPeer, id).Error; err != nil {
		return err
	}

	if err := s.db.WithContext(ctx).Unscoped().Unscoped().Delete(&models.SeedPeer{}, id).Error; err != nil {
		return err
	}

	return nil
}

func (s *service) UpdateSeedPeer(ctx context.Context, id uint, json types.UpdateSeedPeerRequest) (*models.SeedPeer, error) {
	seedPeer := models.SeedPeer{}
	if err := s.db.WithContext(ctx).First(&seedPeer, id).Updates(models.SeedPeer{
		Type:              json.Type,
		IDC:               json.IDC,
		Location:          json.Location,
		IP:                json.IP,
		Port:              json.Port,
		DownloadPort:      json.DownloadPort,
		ObjectStoragePort: json.ObjectStoragePort,
		SeedPeerClusterID: json.SeedPeerClusterID,
	}).Error; err != nil {
		return nil, err
	}

	return &seedPeer, nil
}

func (s *service) GetSeedPeer(ctx context.Context, id uint) (*models.SeedPeer, error) {
	seedPeer := models.SeedPeer{}
	if err := s.db.WithContext(ctx).First(&seedPeer, id).Error; err != nil {
		return nil, err
	}

	return &seedPeer, nil
}

func (s *service) GetSeedPeers(ctx context.Context, q types.GetSeedPeersQuery) ([]models.SeedPeer, int64, error) {
	var count int64
	var seedPeers []models.SeedPeer
	if err := s.db.WithContext(ctx).Scopes(models.Paginate(q.Page, q.PerPage)).Where(&models.SeedPeer{
		Type:              q.Type,
		Hostname:          q.Hostname,
		IDC:               q.IDC,
		Location:          q.Location,
		IP:                q.IP,
		Port:              q.Port,
		DownloadPort:      q.DownloadPort,
		ObjectStoragePort: q.ObjectStoragePort,
		SeedPeerClusterID: q.SeedPeerClusterID,
	}).Find(&seedPeers).Limit(-1).Offset(-1).Count(&count).Error; err != nil {
		return nil, 0, err
	}

	return seedPeers, count, nil
}
