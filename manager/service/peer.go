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

func (s *service) CreatePeer(ctx context.Context, json types.CreatePeerRequest) (*models.Peer, error) {
	peer := models.Peer{
		Hostname:           json.Hostname,
		Type:               json.Type,
		IDC:                json.IDC,
		Location:           json.Location,
		IP:                 json.IP,
		Port:               json.Port,
		DownloadPort:       json.DownloadPort,
		ObjectStoragePort:  json.ObjectStoragePort,
		State:              json.State,
		OS:                 json.OS,
		Platform:           json.Platform,
		PlatformFamily:     json.PlatformFamily,
		PlatformVersion:    json.PlatformVersion,
		KernelVersion:      json.KernelVersion,
		GitVersion:         json.GitVersion,
		GitCommit:          json.GitCommit,
		BuildPlatform:      json.BuildPlatform,
		SchedulerClusterID: json.SchedulerClusterID,
	}

	if err := s.db.WithContext(ctx).Create(&peer).Error; err != nil {
		return nil, err
	}

	return &peer, nil
}

func (s *service) DestroyPeer(ctx context.Context, id uint) error {
	peer := models.Peer{}
	if err := s.db.WithContext(ctx).First(&peer, id).Error; err != nil {
		return err
	}

	if err := s.db.WithContext(ctx).Unscoped().Unscoped().Delete(&models.Peer{}, id).Error; err != nil {
		return err
	}

	return nil
}

func (s *service) GetPeer(ctx context.Context, id uint) (*models.Peer, error) {
	peer := models.Peer{}
	if err := s.db.WithContext(ctx).First(&peer, id).Error; err != nil {
		return nil, err
	}

	return &peer, nil
}

func (s *service) GetPeers(ctx context.Context, q types.GetPeersQuery) ([]models.Peer, int64, error) {
	var count int64
	var peers []models.Peer
	if err := s.db.WithContext(ctx).Preload("SchedulerCluster").Scopes(models.Paginate(q.Page, q.PerPage)).Where(&models.Peer{
		Type:               q.Type,
		Hostname:           q.Hostname,
		IDC:                q.IDC,
		Location:           q.Location,
		IP:                 q.IP,
		Port:               q.Port,
		DownloadPort:       q.DownloadPort,
		ObjectStoragePort:  q.ObjectStoragePort,
		State:              q.State,
		OS:                 q.OS,
		Platform:           q.Platform,
		PlatformFamily:     q.PlatformFamily,
		PlatformVersion:    q.PlatformVersion,
		KernelVersion:      q.KernelVersion,
		GitVersion:         q.GitVersion,
		GitCommit:          q.GitCommit,
		BuildPlatform:      q.BuildPlatform,
		SchedulerClusterID: q.SchedulerClusterID,
	}).Find(&peers).Limit(-1).Offset(-1).Count(&count).Error; err != nil {
		return nil, 0, err
	}

	return peers, count, nil
}
