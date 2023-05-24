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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	cachev8 "github.com/go-redis/cache/v8"
	"github.com/go-redis/redis/v8"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"gorm.io/gorm"

	commonv2 "d7y.io/api/pkg/apis/common/v2"
	managerv2 "d7y.io/api/pkg/apis/manager/v2"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/manager/cache"
	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/manager/database"
	"d7y.io/dragonfly/v2/manager/metrics"
	"d7y.io/dragonfly/v2/manager/models"
	"d7y.io/dragonfly/v2/manager/searcher"
	"d7y.io/dragonfly/v2/manager/types"
	pkgcache "d7y.io/dragonfly/v2/pkg/cache"
	"d7y.io/dragonfly/v2/pkg/objectstorage"
	pkgredis "d7y.io/dragonfly/v2/pkg/redis"
	"d7y.io/dragonfly/v2/pkg/slices"
)

// managerServerV2 is v2 version of the manager grpc server.
type managerServerV2 struct {
	// Manager configuration.
	config *config.Config

	// GORM instance.
	db *gorm.DB

	// Redis universal client interface.
	rdb redis.UniversalClient

	// Cache instance.
	cache *cache.Cache

	// Peer memory cache.
	peerCache pkgcache.Cache

	// Searcher interface.
	searcher searcher.Searcher

	// Object storage interface.
	objectStorage objectstorage.ObjectStorage

	// Object storage configuration.
	objectStorageConfig *config.ObjectStorageConfig
}

// newManagerServerV2 returns v2 version of the manager server.
func newManagerServerV2(
	cfg *config.Config, database *database.Database, cache *cache.Cache, peerCache pkgcache.Cache, searcher searcher.Searcher,
	objectStorage objectstorage.ObjectStorage, objectStorageConfig *config.ObjectStorageConfig,
) managerv2.ManagerServer {
	return &managerServerV2{
		config:              cfg,
		db:                  database.DB,
		rdb:                 database.RDB,
		cache:               cache,
		peerCache:           peerCache,
		searcher:            searcher,
		objectStorage:       objectStorage,
		objectStorageConfig: objectStorageConfig,
	}
}

// Get SeedPeer and SeedPeer cluster configuration.
func (s *managerServerV2) GetSeedPeer(ctx context.Context, req *managerv2.GetSeedPeerRequest) (*managerv2.SeedPeer, error) {
	log := logger.WithHostnameAndIP(req.Hostname, req.Ip)
	cacheKey := pkgredis.MakeSeedPeerKeyInManager(uint(req.SeedPeerClusterId), req.Hostname, req.Ip)

	// Cache hit.
	var pbSeedPeer managerv2.SeedPeer
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
	var pbSchedulers []*managerv2.Scheduler
	for _, schedulerCluster := range seedPeer.SeedPeerCluster.SchedulerClusters {
		for _, scheduler := range schedulerCluster.Schedulers {
			// Marshal features of scheduler.
			features, err := scheduler.Features.MarshalJSON()
			if err != nil {
				return nil, status.Error(codes.DataLoss, err.Error())
			}

			pbSchedulers = append(pbSchedulers, &managerv2.Scheduler{
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
	pbSeedPeer = managerv2.SeedPeer{
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
		SeedPeerCluster: &managerv2.SeedPeerCluster{
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

// Update SeedPeer configuration.
func (s *managerServerV2) UpdateSeedPeer(ctx context.Context, req *managerv2.UpdateSeedPeerRequest) (*managerv2.SeedPeer, error) {
	log := logger.WithHostnameAndIP(req.Hostname, req.Ip)
	seedPeer := models.SeedPeer{}
	if err := s.db.WithContext(ctx).First(&seedPeer, models.SeedPeer{
		Hostname:          req.Hostname,
		SeedPeerClusterID: uint(req.SeedPeerClusterId),
	}).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return s.createSeedPeer(ctx, req)
		}

		return nil, status.Error(codes.Internal, err.Error())
	}

	if err := s.db.WithContext(ctx).Model(&seedPeer).Updates(models.SeedPeer{
		Type:              req.Type,
		IDC:               req.Idc,
		Location:          req.Location,
		IP:                req.Ip,
		Port:              req.Port,
		DownloadPort:      req.DownloadPort,
		ObjectStoragePort: req.ObjectStoragePort,
		SeedPeerClusterID: uint(req.SeedPeerClusterId),
	}).Error; err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	if err := s.cache.Delete(
		ctx,
		pkgredis.MakeSeedPeerKeyInManager(seedPeer.SeedPeerClusterID, seedPeer.Hostname, seedPeer.IP),
	); err != nil {
		log.Warn(err)
	}

	return &managerv2.SeedPeer{
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
func (s *managerServerV2) createSeedPeer(ctx context.Context, req *managerv2.UpdateSeedPeerRequest) (*managerv2.SeedPeer, error) {
	seedPeer := models.SeedPeer{
		Hostname:          req.Hostname,
		Type:              req.Type,
		IDC:               req.Idc,
		Location:          req.Location,
		IP:                req.Ip,
		Port:              req.Port,
		DownloadPort:      req.DownloadPort,
		ObjectStoragePort: req.ObjectStoragePort,
		SeedPeerClusterID: uint(req.SeedPeerClusterId),
	}

	if err := s.db.WithContext(ctx).Create(&seedPeer).Error; err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &managerv2.SeedPeer{
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
func (s *managerServerV2) GetScheduler(ctx context.Context, req *managerv2.GetSchedulerRequest) (*managerv2.Scheduler, error) {
	log := logger.WithHostnameAndIP(req.Hostname, req.Ip)
	cacheKey := pkgredis.MakeSchedulerKeyInManager(uint(req.SchedulerClusterId), req.Hostname, req.Ip)

	// Cache hit.
	var pbScheduler managerv2.Scheduler
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
	var pbSeedPeers []*managerv2.SeedPeer
	for _, seedPeerCluster := range scheduler.SchedulerCluster.SeedPeerClusters {
		seedPeerClusterConfig, err := seedPeerCluster.Config.MarshalJSON()
		if err != nil {
			return nil, status.Error(codes.DataLoss, err.Error())
		}

		for _, seedPeer := range seedPeerCluster.SeedPeers {
			pbSeedPeers = append(pbSeedPeers, &managerv2.SeedPeer{
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
				SeedPeerCluster: &managerv2.SeedPeerCluster{
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
	pbScheduler = managerv2.Scheduler{
		Id:                 uint64(scheduler.ID),
		Hostname:           scheduler.Hostname,
		Idc:                scheduler.IDC,
		Location:           scheduler.Location,
		Ip:                 scheduler.IP,
		Port:               scheduler.Port,
		State:              scheduler.State,
		Features:           features,
		SchedulerClusterId: uint64(scheduler.SchedulerClusterID),
		SchedulerCluster: &managerv2.SchedulerCluster{
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
func (s *managerServerV2) UpdateScheduler(ctx context.Context, req *managerv2.UpdateSchedulerRequest) (*managerv2.Scheduler, error) {
	log := logger.WithHostnameAndIP(req.Hostname, req.Ip)
	scheduler := models.Scheduler{}
	if err := s.db.WithContext(ctx).First(&scheduler, models.Scheduler{
		Hostname:           req.Hostname,
		SchedulerClusterID: uint(req.SchedulerClusterId),
	}).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return s.createScheduler(ctx, req)
		}

		return nil, status.Error(codes.Internal, err.Error())
	}

	if err := s.db.WithContext(ctx).Model(&scheduler).Updates(models.Scheduler{
		IDC:                req.Idc,
		Location:           req.Location,
		IP:                 req.Ip,
		Port:               req.Port,
		SchedulerClusterID: uint(req.SchedulerClusterId),
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

	return &managerv2.Scheduler{
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
func (s *managerServerV2) createScheduler(ctx context.Context, req *managerv2.UpdateSchedulerRequest) (*managerv2.Scheduler, error) {
	scheduler := models.Scheduler{
		Hostname:           req.Hostname,
		IDC:                req.Idc,
		Location:           req.Location,
		IP:                 req.Ip,
		Port:               req.Port,
		Features:           types.DefaultSchedulerFeatures,
		SchedulerClusterID: uint(req.SchedulerClusterId),
	}

	if err := s.db.WithContext(ctx).Create(&scheduler).Error; err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	// Marshal features of scheduler.
	features, err := scheduler.Features.MarshalJSON()
	if err != nil {
		return nil, status.Error(codes.DataLoss, err.Error())
	}

	return &managerv2.Scheduler{
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
func (s *managerServerV2) ListSchedulers(ctx context.Context, req *managerv2.ListSchedulersRequest) (*managerv2.ListSchedulersResponse, error) {
	log := logger.WithHostnameAndIP(req.Hostname, req.Ip)
	log.Debugf("list schedulers, version %s, commit %s", req.Version, req.Commit)
	metrics.SearchSchedulerClusterCount.WithLabelValues(req.Version, req.Commit).Inc()

	// Count the number of the active peer.
	if s.config.Metrics.EnablePeerGauge && req.SourceType == managerv2.SourceType_PEER_SOURCE {
		peerCacheKey := fmt.Sprintf("%s-%s", req.Hostname, req.Ip)
		if data, _, found := s.peerCache.GetWithExpiration(peerCacheKey); !found {
			metrics.PeerGauge.WithLabelValues(req.Version, req.Commit).Inc()
		} else if cache, ok := data.(*managerv2.ListSchedulersRequest); ok && (cache.Version != req.Version || cache.Commit != req.Commit) {
			metrics.PeerGauge.WithLabelValues(cache.Version, cache.Commit).Dec()
			metrics.PeerGauge.WithLabelValues(req.Version, req.Commit).Inc()
		}

		s.peerCache.SetDefault(peerCacheKey, req)
	}

	// Cache hit.
	var pbListSchedulersResponse managerv2.ListSchedulersResponse
	cacheKey := pkgredis.MakeSchedulersKeyForPeerInManager(req.Hostname, req.Ip)

	if err := s.cache.Get(ctx, cacheKey, &pbListSchedulersResponse); err != nil {
		log.Warnf("%s cache miss because of %s", cacheKey, err.Error())
	} else {
		log.Debugf("%s cache hit", cacheKey)
		return &pbListSchedulersResponse, nil
	}

	// Cache miss and search scheduler cluster.
	var schedulerClusters []models.SchedulerCluster
	if err := s.db.WithContext(ctx).Preload("SeedPeerClusters.SeedPeers", "state = ?", "active").Preload("Schedulers", "state = ?", "active").Find(&schedulerClusters).Error; err != nil {
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
		seedPeers := []*managerv2.SeedPeer{}
		for _, seedPeerCluster := range scheduler.SchedulerCluster.SeedPeerClusters {
			for _, seedPeer := range seedPeerCluster.SeedPeers {
				seedPeers = append(seedPeers, &managerv2.SeedPeer{
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

		pbListSchedulersResponse.Schedulers = append(pbListSchedulersResponse.Schedulers, &managerv2.Scheduler{
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
func (s *managerServerV2) GetObjectStorage(ctx context.Context, req *managerv2.GetObjectStorageRequest) (*managerv2.ObjectStorage, error) {
	if !s.objectStorageConfig.Enable {
		return nil, status.Error(codes.NotFound, "object storage is disabled")
	}

	return &managerv2.ObjectStorage{
		Name:             s.objectStorageConfig.Name,
		Region:           s.objectStorageConfig.Region,
		Endpoint:         s.objectStorageConfig.Endpoint,
		AccessKey:        s.objectStorageConfig.AccessKey,
		SecretKey:        s.objectStorageConfig.SecretKey,
		S3ForcePathStyle: s.objectStorageConfig.S3ForcePathStyle,
	}, nil
}

// List buckets configuration.
func (s *managerServerV2) ListBuckets(ctx context.Context, req *managerv2.ListBucketsRequest) (*managerv2.ListBucketsResponse, error) {
	if !s.objectStorageConfig.Enable {
		return nil, status.Error(codes.NotFound, "object storage is disabled")
	}

	log := logger.WithHostnameAndIP(req.Hostname, req.Ip)
	var pbListBucketsResponse managerv2.ListBucketsResponse
	cacheKey := pkgredis.MakeBucketKeyInManager(s.objectStorageConfig.Name)

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
		pbListBucketsResponse.Buckets = append(pbListBucketsResponse.Buckets, &managerv2.Bucket{
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
func (s *managerServerV2) ListApplications(ctx context.Context, req *managerv2.ListApplicationsRequest) (*managerv2.ListApplicationsResponse, error) {
	log := logger.WithHostnameAndIP(req.Hostname, req.Ip)

	// Cache hit.
	var pbListApplicationsResponse managerv2.ListApplicationsResponse
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

		return nil, status.Error(codes.Internal, err.Error())
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

		var pbURLPriorities []*managerv2.URLPriority
		for _, url := range priority.URLs {
			pbURLPriorities = append(pbURLPriorities, &managerv2.URLPriority{
				Regex: url.Regex,
				Value: commonv2.Priority(url.Value),
			})
		}

		pbListApplicationsResponse.Applications = append(pbListApplicationsResponse.Applications, &managerv2.Application{
			Id:   uint64(application.ID),
			Name: application.Name,
			Url:  application.URL,
			Bio:  application.BIO,
			Priority: &managerv2.ApplicationPriority{
				Value: commonv2.Priority(*priority.Value),
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

// TODO(MinH-09) Implement function.
// CreateModel creates model and update data of model to object storage.
func (s *managerServerV2) CreateModel(ctx context.Context, req *managerv2.CreateModelRequest) (*emptypb.Empty, error) {
	return new(emptypb.Empty), nil
}

// KeepAlive with manager.
func (s *managerServerV2) KeepAlive(stream managerv2.Manager_KeepAliveServer) error {
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
	if sourceType == managerv2.SourceType_SCHEDULER_SOURCE {
		scheduler := models.Scheduler{}
		if err := s.db.First(&scheduler, models.Scheduler{
			Hostname:           hostname,
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
	if sourceType == managerv2.SourceType_SEED_PEER_SOURCE {
		seedPeer := models.SeedPeer{}
		if err := s.db.First(&seedPeer, models.SeedPeer{
			Hostname:          hostname,
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
			if sourceType == managerv2.SourceType_SCHEDULER_SOURCE {
				scheduler := models.Scheduler{}
				if err := s.db.First(&scheduler, models.Scheduler{
					Hostname:           hostname,
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
			if sourceType == managerv2.SourceType_SEED_PEER_SOURCE {
				seedPeer := models.SeedPeer{}
				if err := s.db.First(&seedPeer, models.SeedPeer{
					Hostname:          hostname,
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
