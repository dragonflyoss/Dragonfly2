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

	"d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/manager/types"
)

func (s *service) CreateSeedPeer(ctx context.Context, json types.CreateSeedPeerRequest) (*model.SeedPeer, error) {
	seedPeer := model.SeedPeer{
		HostName:          json.HostName,
		Type:              json.Type,
		IDC:               json.IDC,
		NetTopology:       json.NetTopology,
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
	seedPeer := model.SeedPeer{}
	if err := s.db.WithContext(ctx).First(&seedPeer, id).Error; err != nil {
		return err
	}

	if err := s.db.WithContext(ctx).Unscoped().Delete(&model.SeedPeer{}, id).Error; err != nil {
		return err
	}

	return nil
}

func (s *service) UpdateSeedPeer(ctx context.Context, id uint, json types.UpdateSeedPeerRequest) (*model.SeedPeer, error) {
	seedPeer := model.SeedPeer{}
	if err := s.db.WithContext(ctx).First(&seedPeer, id).Updates(model.SeedPeer{
		Type:              json.Type,
		IDC:               json.IDC,
		NetTopology:       json.NetTopology,
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

func (s *service) GetSeedPeer(ctx context.Context, id uint) (*model.SeedPeer, error) {
	seedPeer := model.SeedPeer{}
	if err := s.db.WithContext(ctx).First(&seedPeer, id).Error; err != nil {
		return nil, err
	}

	return &seedPeer, nil
}

func (s *service) GetSeedPeers(ctx context.Context, q types.GetSeedPeersQuery) ([]model.SeedPeer, int64, error) {
	var count int64
	var seedPeers []model.SeedPeer
	if err := s.db.WithContext(ctx).Scopes(model.Paginate(q.Page, q.PerPage)).Where(&model.SeedPeer{
		Type:              q.Type,
		HostName:          q.HostName,
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
