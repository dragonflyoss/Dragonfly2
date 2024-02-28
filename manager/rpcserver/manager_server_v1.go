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

package rpcserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	cachev8 "github.com/go-redis/cache/v8"
	"github.com/go-redis/redis/v8"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"gorm.io/gorm"

	commonv1 "d7y.io/api/v2/pkg/apis/common/v1"
	inference "d7y.io/api/v2/pkg/apis/inference"
	managerv1 "d7y.io/api/v2/pkg/apis/manager/v1"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/manager/cache"
	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/manager/database"
	"d7y.io/dragonfly/v2/manager/metrics"
	"d7y.io/dragonfly/v2/manager/models"
	"d7y.io/dragonfly/v2/manager/searcher"
	"d7y.io/dragonfly/v2/manager/types"
	"d7y.io/dragonfly/v2/pkg/digest"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/objectstorage"
	pkgredis "d7y.io/dragonfly/v2/pkg/redis"
	"d7y.io/dragonfly/v2/pkg/slices"
	"d7y.io/dragonfly/v2/pkg/structure"
)

// managerServerV1 is v1 version of the manager grpc server.
type managerServerV1 struct {
	// Manager configuration.
	config *config.Config

	// GORM instance.
	db *gorm.DB

	// Redis universal client interface.
	rdb redis.UniversalClient

	// Cache instance.
	cache *cache.Cache

	// Searcher interface.
	searcher searcher.Searcher

	// Object storage interface.
	objectStorage objectstorage.ObjectStorage
}

// newManagerServerV1 returns v1 version of the manager server.
func newManagerServerV1(
	cfg *config.Config, database *database.Database, cache *cache.Cache, searcher searcher.Searcher,
	objectStorage objectstorage.ObjectStorage) managerv1.ManagerServer {
	return &managerServerV1{
		config:        cfg,
		db:            database.DB,
		rdb:           database.RDB,
		cache:         cache,
		searcher:      searcher,
		objectStorage: objectStorage,
	}
}

// Get SeedPeer and SeedPeer cluster configuration.
func (s *managerServerV1) GetSeedPeer(ctx context.Context, req *managerv1.GetSeedPeerRequest) (*managerv1.SeedPeer, error) {
	log := logger.WithHostnameAndIP(req.Hostname, req.Ip)
	cacheKey := pkgredis.MakeSeedPeerKeyInManager(uint(req.SeedPeerClusterId), req.Hostname, req.Ip)

	// Cache hit.
	var pbSeedPeer managerv1.SeedPeer
	if err := s.cache.Get(ctx, cacheKey, &pbSeedPeer); err != nil {
		log.Warnf("%s cache miss because of %s", cacheKey, err.Error())
	} else {
		log.Debugf("%s cache hit", cacheKey)
		return &pbSeedPeer, nil
	}

	// Cache miss and search seed peer.
	seedPeer := models.SeedPeer{}
	if err := s.db.WithContext(ctx).Preload("SeedPeerCluster").Preload("SeedPeerCluster.SchedulerClusters.Schedulers", &models.Scheduler{
		State: models.SchedulerStateActive,
	}).First(&seedPeer, &models.SeedPeer{
		Hostname:          req.Hostname,
		SeedPeerClusterID: uint(req.SeedPeerClusterId),
	}).Error; err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	// Marshal config of seed peer cluster.
	config, err := seedPeer.SeedPeerCluster.Config.MarshalJSON()
	if err != nil {
		return nil, status.Error(codes.DataLoss, err.Error())
	}

	// Construct schedulers.
	var pbSchedulers []*managerv1.Scheduler
	for _, schedulerCluster := range seedPeer.SeedPeerCluster.SchedulerClusters {
		for _, scheduler := range schedulerCluster.Schedulers {
			// Marshal features of scheduler.
			features, err := scheduler.Features.MarshalJSON()
			if err != nil {
				return nil, status.Error(codes.DataLoss, err.Error())
			}

			pbSchedulers = append(pbSchedulers, &managerv1.Scheduler{
				Id:       uint64(scheduler.ID),
				Hostname: scheduler.Hostname,
				Idc:      scheduler.IDC,
				Location: scheduler.Location,
				Ip:       scheduler.IP,
				Port:     scheduler.Port,
				State:    scheduler.State,
				Features: features,
			})
		}
	}

	// Construct seed peer.
	pbSeedPeer = managerv1.SeedPeer{
		Id:                uint64(seedPeer.ID),
		Type:              seedPeer.Type,
		Hostname:          seedPeer.Hostname,
		Idc:               seedPeer.IDC,
		Location:          seedPeer.Location,
		Ip:                seedPeer.IP,
		Port:              seedPeer.Port,
		DownloadPort:      seedPeer.DownloadPort,
		ObjectStoragePort: seedPeer.ObjectStoragePort,
		State:             seedPeer.State,
		SeedPeerClusterId: uint64(seedPeer.SeedPeerClusterID),
		SeedPeerCluster: &managerv1.SeedPeerCluster{
			Id:     uint64(seedPeer.SeedPeerCluster.ID),
			Name:   seedPeer.SeedPeerCluster.Name,
			Bio:    seedPeer.SeedPeerCluster.BIO,
			Config: config,
		},
		Schedulers: pbSchedulers,
	}

	// Cache data.
	if err := s.cache.Once(&cachev8.Item{
		Ctx:   ctx,
		Key:   cacheKey,
		Value: &pbSeedPeer,
		TTL:   s.cache.TTL,
	}); err != nil {
		log.Error(err)
	}

	return &pbSeedPeer, nil
}

// List acitve seed peers configuration.
func (s *managerServerV1) ListSeedPeers(ctx context.Context, req *managerv1.ListSeedPeersRequest) (*managerv1.ListSeedPeersResponse, error) {
	log := logger.WithHostnameAndIP(req.Hostname, req.Ip)
	log.Debugf("list seed peers, version %s, commit %s", req.Version, req.Commit)

	// Cache hit.
	var pbListSeedPeersResponse managerv1.ListSeedPeersResponse
	cacheKey := pkgredis.MakeSeedPeersKeyForPeerInManager(req.Hostname, req.Ip)

	if err := s.cache.Get(ctx, cacheKey, &pbListSeedPeersResponse); err != nil {
		log.Warnf("%s cache miss because of %s", cacheKey, err.Error())
	} else {
		log.Debugf("%s cache hit", cacheKey)
		return &pbListSeedPeersResponse, nil
	}

	// Cache miss and search seed peer.
	seedPeer := models.SeedPeer{}
	if err := s.db.WithContext(ctx).Preload("SeedPeerCluster").First(&seedPeer, models.SeedPeer{
		Hostname: req.Hostname,
		IP:       req.Ip,
	}).Error; err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	var seedPeers []models.SeedPeer
	if err := s.db.WithContext(ctx).Find(&seedPeers, models.SeedPeer{
		State:             models.SeedPeerStateActive,
		SeedPeerClusterID: uint(seedPeer.SeedPeerClusterID),
	}).Error; err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	if len(seedPeers) == 0 {
		return nil, status.Error(codes.NotFound, "seed peer not found")
	}

	// Construct seed peers.
	for _, seedPeer := range seedPeers {
		pbListSeedPeersResponse.SeedPeers = append(pbListSeedPeersResponse.SeedPeers, &managerv1.SeedPeer{
			Id:                uint64(seedPeer.ID),
			Hostname:          seedPeer.Hostname,
			Type:              seedPeer.Type,
			Idc:               seedPeer.IDC,
			Location:          seedPeer.Location,
			Ip:                seedPeer.IP,
			Port:              seedPeer.Port,
			DownloadPort:      seedPeer.DownloadPort,
			ObjectStoragePort: seedPeer.ObjectStoragePort,
			State:             seedPeer.State,
			SeedPeerClusterId: uint64(seedPeer.SeedPeerClusterID),
			SeedPeerCluster: &managerv1.SeedPeerCluster{
				Id:   uint64(seedPeer.SeedPeerCluster.ID),
				Name: seedPeer.SeedPeerCluster.Name,
				Bio:  seedPeer.SeedPeerCluster.BIO,
			},
		})
	}

	// Cache data.
	if err := s.cache.Once(&cachev8.Item{
		Ctx:   ctx,
		Key:   cacheKey,
		Value: &pbListSeedPeersResponse,
		TTL:   s.cache.TTL,
	}); err != nil {
		log.Error(err)
	}

	return &pbListSeedPeersResponse, nil
}

// Update SeedPeer configuration.
func (s *managerServerV1) UpdateSeedPeer(ctx context.Context, req *managerv1.UpdateSeedPeerRequest) (*managerv1.SeedPeer, error) {
	log := logger.WithHostnameAndIP(req.Hostname, req.Ip)
	seedPeer := models.SeedPeer{}
	if err := s.db.WithContext(ctx).First(&seedPeer, models.SeedPeer{
		Hostname:          req.Hostname,
		IP:                req.Ip,
		SeedPeerClusterID: uint(req.SeedPeerClusterId),
	}).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return s.createSeedPeer(ctx, req)
		}

		return nil, status.Error(codes.Internal, err.Error())
	}

	if err := s.db.WithContext(ctx).Model(&seedPeer).Updates(models.SeedPeer{
		Type:              req.GetType(),
		IDC:               req.GetIdc(),
		Location:          req.GetLocation(),
		IP:                req.GetIp(),
		Port:              req.GetPort(),
		DownloadPort:      req.GetDownloadPort(),
		ObjectStoragePort: req.GetObjectStoragePort(),
		State:             models.SeedPeerStateActive,
		SeedPeerClusterID: uint(req.GetSeedPeerClusterId()),
	}).Error; err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	if err := s.cache.Delete(
		ctx,
		pkgredis.MakeSeedPeerKeyInManager(seedPeer.SeedPeerClusterID, seedPeer.Hostname, seedPeer.IP),
	); err != nil {
		log.Warn(err)
	}

	return &managerv1.SeedPeer{
		Id:                uint64(seedPeer.ID),
		Hostname:          seedPeer.Hostname,
		Type:              seedPeer.Type,
		Idc:               seedPeer.IDC,
		Location:          seedPeer.Location,
		Ip:                seedPeer.IP,
		Port:              seedPeer.Port,
		DownloadPort:      seedPeer.DownloadPort,
		ObjectStoragePort: seedPeer.ObjectStoragePort,
		State:             seedPeer.State,
		SeedPeerClusterId: uint64(seedPeer.SeedPeerClusterID),
	}, nil
}

// Create SeedPeer and associate cluster.
func (s *managerServerV1) createSeedPeer(ctx context.Context, req *managerv1.UpdateSeedPeerRequest) (*managerv1.SeedPeer, error) {
	seedPeer := models.SeedPeer{
		Hostname:          req.GetHostname(),
		Type:              req.GetType(),
		IDC:               req.GetIdc(),
		Location:          req.GetLocation(),
		IP:                req.GetIp(),
		Port:              req.GetPort(),
		DownloadPort:      req.GetDownloadPort(),
		ObjectStoragePort: req.GetObjectStoragePort(),
		SeedPeerClusterID: uint(req.GetSeedPeerClusterId()),
	}

	if err := s.db.WithContext(ctx).Create(&seedPeer).Error; err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &managerv1.SeedPeer{
		Id:                uint64(seedPeer.ID),
		Hostname:          seedPeer.Hostname,
		Type:              seedPeer.Type,
		Idc:               seedPeer.IDC,
		Location:          seedPeer.Location,
		Ip:                seedPeer.IP,
		Port:              seedPeer.Port,
		DownloadPort:      seedPeer.DownloadPort,
		ObjectStoragePort: seedPeer.ObjectStoragePort,
		SeedPeerClusterId: uint64(seedPeer.SeedPeerClusterID),
		State:             seedPeer.State,
	}, nil
}

// Get Scheduler and Scheduler cluster configuration.
func (s *managerServerV1) GetScheduler(ctx context.Context, req *managerv1.GetSchedulerRequest) (*managerv1.Scheduler, error) {
	log := logger.WithHostnameAndIP(req.Hostname, req.Ip)
	cacheKey := pkgredis.MakeSchedulerKeyInManager(uint(req.SchedulerClusterId), req.Hostname, req.Ip)

	// Cache hit.
	var pbScheduler managerv1.Scheduler
	if err := s.cache.Get(ctx, cacheKey, &pbScheduler); err != nil {
		log.Warnf("%s cache miss because of %s", cacheKey, err.Error())
	} else {
		log.Debugf("%s cache hit", cacheKey)
		return &pbScheduler, nil
	}

	// Cache miss and search scheduler.
	scheduler := models.Scheduler{}
	if err := s.db.WithContext(ctx).Preload("SchedulerCluster").Preload("SchedulerCluster.SeedPeerClusters.SeedPeers", &models.SeedPeer{
		State: models.SeedPeerStateActive,
	}).First(&scheduler, &models.Scheduler{
		Hostname:           req.Hostname,
		SchedulerClusterID: uint(req.SchedulerClusterId),
	}).Error; err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	// Marshal config of scheduler.
	schedulerClusterConfig, err := scheduler.SchedulerCluster.Config.MarshalJSON()
	if err != nil {
		return nil, status.Error(codes.DataLoss, err.Error())
	}

	// Marshal config of client.
	schedulerClusterClientConfig, err := scheduler.SchedulerCluster.ClientConfig.MarshalJSON()
	if err != nil {
		return nil, status.Error(codes.DataLoss, err.Error())
	}

	// Marshal scopes of client.
	schedulerClusterScopes, err := scheduler.SchedulerCluster.Scopes.MarshalJSON()
	if err != nil {
		return nil, status.Error(codes.DataLoss, err.Error())
	}

	// Construct seed peers.
	var pbSeedPeers []*managerv1.SeedPeer
	for _, seedPeerCluster := range scheduler.SchedulerCluster.SeedPeerClusters {
		seedPeerClusterConfig, err := seedPeerCluster.Config.MarshalJSON()
		if err != nil {
			return nil, status.Error(codes.DataLoss, err.Error())
		}

		for _, seedPeer := range seedPeerCluster.SeedPeers {
			pbSeedPeers = append(pbSeedPeers, &managerv1.SeedPeer{
				Id:                uint64(seedPeer.ID),
				Hostname:          seedPeer.Hostname,
				Type:              seedPeer.Type,
				Idc:               seedPeer.IDC,
				Location:          seedPeer.Location,
				Ip:                seedPeer.IP,
				Port:              seedPeer.Port,
				DownloadPort:      seedPeer.DownloadPort,
				ObjectStoragePort: seedPeer.ObjectStoragePort,
				State:             seedPeer.State,
				SeedPeerClusterId: uint64(seedPeer.SeedPeerClusterID),
				SeedPeerCluster: &managerv1.SeedPeerCluster{
					Id:     uint64(seedPeerCluster.ID),
					Name:   seedPeerCluster.Name,
					Bio:    seedPeerCluster.BIO,
					Config: seedPeerClusterConfig,
				},
			})
		}
	}

	// Marshal features of scheduler.
	features, err := scheduler.Features.MarshalJSON()
	if err != nil {
		return nil, status.Error(codes.DataLoss, err.Error())
	}

	// Construct scheduler.
	pbScheduler = managerv1.Scheduler{
		Id:                 uint64(scheduler.ID),
		Hostname:           scheduler.Hostname,
		Idc:                scheduler.IDC,
		Location:           scheduler.Location,
		Ip:                 scheduler.IP,
		Port:               scheduler.Port,
		State:              scheduler.State,
		Features:           features,
		SchedulerClusterId: uint64(scheduler.SchedulerClusterID),
		SchedulerCluster: &managerv1.SchedulerCluster{
			Id:           uint64(scheduler.SchedulerCluster.ID),
			Name:         scheduler.SchedulerCluster.Name,
			Bio:          scheduler.SchedulerCluster.BIO,
			Config:       schedulerClusterConfig,
			ClientConfig: schedulerClusterClientConfig,
			Scopes:       schedulerClusterScopes,
		},
		SeedPeers: pbSeedPeers,
	}

	// Cache data.
	if err := s.cache.Once(&cachev8.Item{
		Ctx:   ctx,
		Key:   cacheKey,
		Value: &pbScheduler,
		TTL:   s.cache.TTL,
	}); err != nil {
		log.Error(err)
	}

	return &pbScheduler, nil
}

// Update scheduler configuration.
func (s *managerServerV1) UpdateScheduler(ctx context.Context, req *managerv1.UpdateSchedulerRequest) (*managerv1.Scheduler, error) {
	log := logger.WithHostnameAndIP(req.Hostname, req.Ip)
	scheduler := models.Scheduler{}
	if err := s.db.WithContext(ctx).First(&scheduler, models.Scheduler{
		Hostname:           req.Hostname,
		IP:                 req.Ip,
		SchedulerClusterID: uint(req.SchedulerClusterId),
	}).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return s.createScheduler(ctx, req)
		}

		return nil, status.Error(codes.Internal, err.Error())
	}

	if err := s.db.WithContext(ctx).Model(&scheduler).Updates(models.Scheduler{
		IDC:                req.GetIdc(),
		Location:           req.GetLocation(),
		IP:                 req.GetIp(),
		Port:               req.GetPort(),
		SchedulerClusterID: uint(req.GetSchedulerClusterId()),
	}).Error; err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	if err := s.cache.Delete(
		ctx,
		pkgredis.MakeSchedulerKeyInManager(scheduler.SchedulerClusterID, scheduler.Hostname, scheduler.IP),
	); err != nil {
		log.Warn(err)
	}

	// Marshal features of scheduler.
	features, err := scheduler.Features.MarshalJSON()
	if err != nil {
		return nil, status.Error(codes.DataLoss, err.Error())
	}

	return &managerv1.Scheduler{
		Id:                 uint64(scheduler.ID),
		Hostname:           scheduler.Hostname,
		Idc:                scheduler.IDC,
		Location:           scheduler.Location,
		Ip:                 scheduler.IP,
		Port:               scheduler.Port,
		Features:           features,
		State:              scheduler.State,
		SchedulerClusterId: uint64(scheduler.SchedulerClusterID),
	}, nil
}

// Create scheduler and associate cluster.
func (s *managerServerV1) createScheduler(ctx context.Context, req *managerv1.UpdateSchedulerRequest) (*managerv1.Scheduler, error) {
	scheduler := models.Scheduler{
		Hostname:           req.GetHostname(),
		IDC:                req.GetIdc(),
		Location:           req.GetLocation(),
		IP:                 req.GetIp(),
		Port:               req.GetPort(),
		Features:           types.DefaultSchedulerFeatures,
		SchedulerClusterID: uint(req.GetSchedulerClusterId()),
	}

	if err := s.db.WithContext(ctx).Create(&scheduler).Error; err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	// Marshal features of scheduler.
	features, err := scheduler.Features.MarshalJSON()
	if err != nil {
		return nil, status.Error(codes.DataLoss, err.Error())
	}

	return &managerv1.Scheduler{
		Id:                 uint64(scheduler.ID),
		Hostname:           scheduler.Hostname,
		Idc:                scheduler.IDC,
		Location:           scheduler.Location,
		Ip:                 scheduler.IP,
		Port:               scheduler.Port,
		State:              scheduler.State,
		Features:           features,
		SchedulerClusterId: uint64(scheduler.SchedulerClusterID),
	}, nil
}

// List acitve schedulers configuration.
func (s *managerServerV1) ListSchedulers(ctx context.Context, req *managerv1.ListSchedulersRequest) (*managerv1.ListSchedulersResponse, error) {
	log := logger.WithHostnameAndIP(req.Hostname, req.Ip)
	log.Debugf("list schedulers, version %s, commit %s", req.Version, req.Commit)
	metrics.SearchSchedulerClusterCount.WithLabelValues(req.Version, req.Commit).Inc()

	// Cache hit.
	var pbListSchedulersResponse managerv1.ListSchedulersResponse
	cacheKey := pkgredis.MakeSchedulersKeyForPeerInManager(req.Hostname, req.Ip)

	if err := s.cache.Get(ctx, cacheKey, &pbListSchedulersResponse); err != nil {
		log.Warnf("%s cache miss because of %s", cacheKey, err.Error())
	} else {
		log.Debugf("%s cache hit", cacheKey)
		return &pbListSchedulersResponse, nil
	}

	// Cache miss and search scheduler cluster.
	var schedulerClusters []models.SchedulerCluster
	if err := s.db.WithContext(ctx).Preload("SeedPeerClusters.SeedPeers", "state = ?", "active").
		Preload("Schedulers", "state = ?", "active").Find(&schedulerClusters).Error; err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	// Remove schedulers which not have scehdule featrue. As OceanBase does not support JSON type,
	// it is not possible to use datatypes.JSONQuery for filtering.
	var tmpSchedulerClusters []models.SchedulerCluster
	for _, schedulerCluster := range schedulerClusters {
		var tmpSchedulers []models.Scheduler
		for _, scheduler := range schedulerCluster.Schedulers {
			if slices.Contains(scheduler.Features, types.SchedulerFeatureSchedule) {
				tmpSchedulers = append(tmpSchedulers, scheduler)
			}
		}

		if len(tmpSchedulers) != 0 {
			schedulerCluster.Schedulers = tmpSchedulers
			tmpSchedulerClusters = append(tmpSchedulerClusters, schedulerCluster)
		}
	}
	log.Debugf("list scheduler clusters %v with hostInfo %#v", getSchedulerClusterNames(tmpSchedulerClusters), req.HostInfo)

	// Search optimal scheduler clusters.
	// If searcher can not found candidate scheduler cluster,
	// return all scheduler clusters.
	var (
		candidateSchedulerClusters []models.SchedulerCluster
		err                        error
	)
	candidateSchedulerClusters, err = s.searcher.FindSchedulerClusters(ctx, tmpSchedulerClusters, req.Ip, req.Hostname, req.HostInfo, logger.CoreLogger)
	if err != nil {
		log.Error(err)
		metrics.SearchSchedulerClusterFailureCount.WithLabelValues(req.Version, req.Commit).Inc()
		candidateSchedulerClusters = schedulerClusters
	}
	log.Debugf("find matching scheduler cluster %v", getSchedulerClusterNames(candidateSchedulerClusters))

	schedulers := []models.Scheduler{}
	for _, candidateSchedulerCluster := range candidateSchedulerClusters {
		for _, scheduler := range candidateSchedulerCluster.Schedulers {
			scheduler.SchedulerCluster = candidateSchedulerCluster
			schedulers = append(schedulers, scheduler)
		}
	}

	// Construct schedulers.
	for _, scheduler := range schedulers {
		seedPeers := []*managerv1.SeedPeer{}
		for _, seedPeerCluster := range scheduler.SchedulerCluster.SeedPeerClusters {
			for _, seedPeer := range seedPeerCluster.SeedPeers {
				seedPeers = append(seedPeers, &managerv1.SeedPeer{
					Id:                uint64(seedPeer.ID),
					Hostname:          seedPeer.Hostname,
					Type:              seedPeer.Type,
					Idc:               seedPeer.IDC,
					Location:          seedPeer.Location,
					Ip:                seedPeer.IP,
					Port:              seedPeer.Port,
					DownloadPort:      seedPeer.DownloadPort,
					ObjectStoragePort: seedPeer.ObjectStoragePort,
					State:             seedPeer.State,
					SeedPeerClusterId: uint64(seedPeer.SeedPeerClusterID),
				})
			}
		}

		// Marshal features of scheduler.
		features, err := scheduler.Features.MarshalJSON()
		if err != nil {
			return nil, status.Error(codes.DataLoss, err.Error())
		}

		pbListSchedulersResponse.Schedulers = append(pbListSchedulersResponse.Schedulers, &managerv1.Scheduler{
			Id:                 uint64(scheduler.ID),
			Hostname:           scheduler.Hostname,
			Idc:                scheduler.IDC,
			Location:           scheduler.Location,
			Ip:                 scheduler.IP,
			Port:               scheduler.Port,
			State:              scheduler.State,
			Features:           features,
			SchedulerClusterId: uint64(scheduler.SchedulerClusterID),
			SeedPeers:          seedPeers,
		})
	}

	// If scheduler is not found, even no default scheduler is returned.
	// It means that the scheduler has not been started,
	// and the results are not cached, waiting for the scheduler to be ready.
	if len(pbListSchedulersResponse.Schedulers) == 0 {
		return &pbListSchedulersResponse, nil
	}

	// Cache data.
	if err := s.cache.Once(&cachev8.Item{
		Ctx:   ctx,
		Key:   cacheKey,
		Value: &pbListSchedulersResponse,
		TTL:   s.cache.TTL,
	}); err != nil {
		log.Error(err)
	}

	return &pbListSchedulersResponse, nil
}

// Get object storage configuration.
func (s *managerServerV1) GetObjectStorage(ctx context.Context, req *managerv1.GetObjectStorageRequest) (*managerv1.ObjectStorage, error) {
	if !s.config.ObjectStorage.Enable {
		return nil, status.Error(codes.Internal, "object storage is disabled")
	}

	return &managerv1.ObjectStorage{
		Name:             s.config.ObjectStorage.Name,
		Region:           s.config.ObjectStorage.Region,
		Endpoint:         s.config.ObjectStorage.Endpoint,
		AccessKey:        s.config.ObjectStorage.AccessKey,
		SecretKey:        s.config.ObjectStorage.SecretKey,
		S3ForcePathStyle: s.config.ObjectStorage.S3ForcePathStyle,
	}, nil
}

// List buckets configuration.
func (s *managerServerV1) ListBuckets(ctx context.Context, req *managerv1.ListBucketsRequest) (*managerv1.ListBucketsResponse, error) {
	log := logger.WithHostnameAndIP(req.Hostname, req.Ip)

	if !s.config.ObjectStorage.Enable {
		log.Warn("object storage is disabled")
		return nil, status.Error(codes.Internal, "object storage is disabled")
	}

	var pbListBucketsResponse managerv1.ListBucketsResponse
	cacheKey := pkgredis.MakeBucketKeyInManager(s.config.ObjectStorage.Name)

	// Cache hit.
	if err := s.cache.Get(ctx, cacheKey, &pbListBucketsResponse); err != nil {
		log.Warnf("%s cache miss because of %s", cacheKey, err.Error())
	} else {
		log.Debugf("%s cache hit", cacheKey)
		return &pbListBucketsResponse, nil
	}

	// Cache miss and search buckets.
	buckets, err := s.objectStorage.ListBucketMetadatas(ctx)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	// Construct schedulers.
	for _, bucket := range buckets {
		pbListBucketsResponse.Buckets = append(pbListBucketsResponse.Buckets, &managerv1.Bucket{
			Name: bucket.Name,
		})
	}

	// Cache data.
	if err := s.cache.Once(&cachev8.Item{
		Ctx:   ctx,
		Key:   cacheKey,
		Value: &pbListBucketsResponse,
		TTL:   s.cache.TTL,
	}); err != nil {
		log.Error(err)
	}

	return &pbListBucketsResponse, nil
}

// List applications configuration.
func (s *managerServerV1) ListApplications(ctx context.Context, req *managerv1.ListApplicationsRequest) (*managerv1.ListApplicationsResponse, error) {
	log := logger.WithHostnameAndIP(req.Hostname, req.Ip)

	// Cache hit.
	var pbListApplicationsResponse managerv1.ListApplicationsResponse
	cacheKey := pkgredis.MakeApplicationsKeyInManager()
	if err := s.cache.Get(ctx, cacheKey, &pbListApplicationsResponse); err != nil {
		log.Warnf("%s cache miss because of %s", cacheKey, err.Error())
	} else {
		log.Debugf("%s cache hit", cacheKey)
		return &pbListApplicationsResponse, nil
	}

	// Cache miss and search applications.
	var applications []models.Application
	if err := s.db.WithContext(ctx).Find(&applications, "priority != ?", "").Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.NotFound, err.Error())
		}

		return nil, status.Error(codes.Unknown, err.Error())
	}

	if len(applications) == 0 {
		return nil, status.Error(codes.NotFound, "application not found")
	}

	for _, application := range applications {
		b, err := application.Priority.MarshalJSON()
		if err != nil {
			log.Warn(err)
			continue
		}

		var priority types.PriorityConfig
		if err := json.Unmarshal(b, &priority); err != nil {
			log.Warn(err)
			continue
		}

		var pbURLPriorities []*managerv1.URLPriority
		for _, url := range priority.URLs {
			pbURLPriorities = append(pbURLPriorities, &managerv1.URLPriority{
				Regex: url.Regex,
				Value: commonv1.Priority(url.Value),
			})
		}

		pbListApplicationsResponse.Applications = append(pbListApplicationsResponse.Applications, &managerv1.Application{
			Id:   uint64(application.ID),
			Name: application.Name,
			Url:  application.URL,
			Bio:  application.BIO,
			Priority: &managerv1.ApplicationPriority{
				Value: commonv1.Priority(*priority.Value),
				Urls:  pbURLPriorities,
			},
		})
	}

	// Cache data.
	if err := s.cache.Once(&cachev8.Item{
		Ctx:   ctx,
		Key:   cacheKey,
		Value: &pbListApplicationsResponse,
		TTL:   s.cache.TTL,
	}); err != nil {
		log.Error(err)
	}

	return &pbListApplicationsResponse, nil
}

// CreateModel creates model and update data of model to object storage.
func (s *managerServerV1) CreateModel(ctx context.Context, req *managerv1.CreateModelRequest) (*emptypb.Empty, error) {
	log := logger.WithHostnameAndIP(req.GetHostname(), req.GetIp())

	if !s.config.ObjectStorage.Enable {
		log.Warn("object storage is disabled")
		return nil, status.Error(codes.Internal, "object storage is disabled")
	}

	// Create model bucket, if not exist.
	if err := s.createModelBucket(ctx); err != nil {
		log.Error(err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	var (
		name       string
		typ        string
		evaluation types.ModelEvaluation
		version    = time.Now().Nanosecond()
	)
	switch createModelRequest := req.GetRequest().(type) {
	case *managerv1.CreateModelRequest_CreateGnnRequest:
		name = idgen.GNNModelIDV1(req.GetIp(), req.GetHostname())
		typ = models.ModelTypeGNN
		evaluation = types.ModelEvaluation{
			Precision: createModelRequest.CreateGnnRequest.GetPrecision(),
			Recall:    createModelRequest.CreateGnnRequest.GetRecall(),
			F1Score:   createModelRequest.CreateGnnRequest.GetF1Score(),
		}

		// Update GNN model config to object storage.
		if err := s.createModelConfig(ctx, name); err != nil {
			log.Error(err)
			return nil, status.Error(codes.Internal, err.Error())
		}

		// Upload GNN model file to object storage.
		data := createModelRequest.CreateGnnRequest.GetData()
		dgst := digest.New(digest.AlgorithmSHA256, digest.SHA256FromBytes(data))
		if err := s.objectStorage.PutObject(ctx, s.config.Trainer.BucketName,
			types.MakeObjectKeyOfModelFile(name, version), dgst.String(), bytes.NewReader(data)); err != nil {
			log.Error(err)
			return nil, status.Error(codes.Internal, err.Error())
		}
	case *managerv1.CreateModelRequest_CreateMlpRequest:
		name = idgen.MLPModelIDV1(req.GetHostname(), req.GetIp())
		typ = models.ModelTypeMLP
		evaluation = types.ModelEvaluation{
			MSE: createModelRequest.CreateMlpRequest.GetMse(),
			MAE: createModelRequest.CreateMlpRequest.GetMae(),
		}

		// Update MLP model config to object storage.
		if err := s.createModelConfig(ctx, name); err != nil {
			log.Error(err)
			return nil, status.Error(codes.Internal, err.Error())
		}

		// Upload MLP model file to object storage.
		data := createModelRequest.CreateMlpRequest.GetData()
		dgst := digest.New(digest.AlgorithmSHA256, digest.SHA256FromBytes(data))
		if err := s.objectStorage.PutObject(ctx, s.config.Trainer.BucketName,
			types.MakeObjectKeyOfModelFile(name, version), dgst.String(), bytes.NewReader(data)); err != nil {
			log.Error(err)
			return nil, status.Error(codes.Internal, err.Error())
		}
	default:
		msg := fmt.Sprintf("receive unknow request: %#v", createModelRequest)
		log.Error(msg)
		return nil, status.Error(codes.FailedPrecondition, msg)
	}

	scheduler := models.Scheduler{}
	if err := s.db.WithContext(ctx).First(&scheduler, &models.Scheduler{
		Hostname: req.Hostname,
		IP:       req.Ip,
	}).Error; err != nil {
		log.Error(err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	rawEvaluation, err := structure.StructToMap(evaluation)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	// Create model in database.
	if err := s.db.WithContext(ctx).Model(&scheduler).Association("Models").Append(&models.Model{
		Name:       name,
		Type:       typ,
		Version:    fmt.Sprint(version),
		State:      models.ModelVersionStateInactive,
		Evaluation: rawEvaluation,
	}); err != nil {
		log.Error(err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	return new(emptypb.Empty), nil
}

// createModelBucket creates model bucket if not exist.
func (s *managerServerV1) createModelBucket(ctx context.Context) error {
	// Check bucket exist.
	isExist, err := s.objectStorage.IsBucketExist(ctx, s.config.Trainer.BucketName)
	if err != nil {
		return err
	}

	// Create bucket if not exist.
	if !isExist {
		if err := s.objectStorage.CreateBucket(ctx, s.config.Trainer.BucketName); err != nil {
			return err
		}
	}

	return nil
}

// createModelConfig creates model config to object storage.
func (s *managerServerV1) createModelConfig(ctx context.Context, name string) error {
	objectKey := types.MakeObjectKeyOfModelConfigFile(name)
	isExist, err := s.objectStorage.IsObjectExist(ctx, s.config.Trainer.BucketName, objectKey)
	if err != nil {
		return err
	}

	// If the model config already exists, skip it.
	if isExist {
		return nil
	}

	// If the model config does not exist, create a new model config.
	pbModelConfig := inference.ModelConfig{
		Name:     name,
		Platform: types.DefaultTritonPlatform,
		VersionPolicy: &inference.ModelVersionPolicy{
			PolicyChoice: &inference.ModelVersionPolicy_Specific_{
				Specific: &inference.ModelVersionPolicy_Specific{},
			},
		},
	}

	dgst := digest.New(digest.AlgorithmSHA256, digest.SHA256FromStrings(pbModelConfig.String()))
	if err := s.objectStorage.PutObject(ctx, s.config.Trainer.BucketName,
		types.MakeObjectKeyOfModelConfigFile(name), dgst.String(), strings.NewReader(pbModelConfig.String())); err != nil {
		return err
	}

	return nil
}

// KeepAlive with manager.
func (s *managerServerV1) KeepAlive(stream managerv1.Manager_KeepAliveServer) error {
	req, err := stream.Recv()
	if err != nil {
		logger.Errorf("keepalive failed for the first time: %s", err.Error())
		return status.Error(codes.Internal, err.Error())
	}
	hostname := req.Hostname
	ip := req.Ip
	sourceType := req.SourceType
	clusterID := uint(req.ClusterId)

	log := logger.WithKeepAlive(hostname, ip, sourceType.Enum().String(), req.ClusterId)
	log.Info("keepalive for the first time")

	// Initialize active scheduler.
	if sourceType == managerv1.SourceType_SCHEDULER_SOURCE {
		scheduler := models.Scheduler{}
		if err := s.db.First(&scheduler, models.Scheduler{
			Hostname:           hostname,
			IP:                 ip,
			SchedulerClusterID: clusterID,
		}).Updates(models.Scheduler{
			State: models.SchedulerStateActive,
		}).Error; err != nil {
			return status.Error(codes.Internal, err.Error())
		}

		if err := s.cache.Delete(
			context.TODO(),
			pkgredis.MakeSchedulerKeyInManager(clusterID, hostname, ip),
		); err != nil {
			log.Warnf("refresh keepalive status failed: %s", err.Error())
		}
	}

	// Initialize active seed peer.
	if sourceType == managerv1.SourceType_SEED_PEER_SOURCE {
		seedPeer := models.SeedPeer{}
		if err := s.db.First(&seedPeer, models.SeedPeer{
			Hostname:          hostname,
			IP:                ip,
			SeedPeerClusterID: clusterID,
		}).Updates(models.SeedPeer{
			State: models.SeedPeerStateActive,
		}).Error; err != nil {
			return status.Error(codes.Internal, err.Error())
		}

		if err := s.cache.Delete(
			context.TODO(),
			pkgredis.MakeSeedPeerKeyInManager(clusterID, hostname, ip),
		); err != nil {
			log.Warnf("refresh keepalive status failed: %s", err.Error())
		}
	}

	for {
		_, err := stream.Recv()
		if err != nil {
			// Inactive scheduler.
			if sourceType == managerv1.SourceType_SCHEDULER_SOURCE {
				scheduler := models.Scheduler{}
				if err := s.db.First(&scheduler, models.Scheduler{
					Hostname:           hostname,
					IP:                 ip,
					SchedulerClusterID: clusterID,
				}).Updates(models.Scheduler{
					State: models.SchedulerStateInactive,
				}).Error; err != nil {
					return status.Error(codes.Internal, err.Error())
				}

				if err := s.cache.Delete(
					context.TODO(),
					pkgredis.MakeSchedulerKeyInManager(clusterID, hostname, ip),
				); err != nil {
					log.Warnf("refresh keepalive status failed: %s", err.Error())
				}
			}

			// Inactive seed peer.
			if sourceType == managerv1.SourceType_SEED_PEER_SOURCE {
				seedPeer := models.SeedPeer{}
				if err := s.db.First(&seedPeer, models.SeedPeer{
					Hostname:          hostname,
					IP:                ip,
					SeedPeerClusterID: clusterID,
				}).Updates(models.SeedPeer{
					State: models.SeedPeerStateInactive,
				}).Error; err != nil {
					return status.Error(codes.Internal, err.Error())
				}

				if err := s.cache.Delete(
					context.TODO(),
					pkgredis.MakeSeedPeerKeyInManager(clusterID, hostname, ip),
				); err != nil {
					log.Warnf("refresh keepalive status failed: %s", err.Error())
				}
			}

			if err == io.EOF {
				log.Info("keepalive closed")
				return nil
			}

			log.Errorf("keepalive failed: %s", err.Error())
			return status.Error(codes.Unknown, err.Error())
		}
	}
}
