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
	log := logger.WithHostnameAndIP(req.HostName, req.Ip)
	cacheKey := cache.MakeSeedPeerCacheKey(uint(req.SeedPeerClusterId), req.HostName, req.Ip)

	// Cache hit.
	var pbSeedPeer managerv2.SeedPeer
	if err := s.cache.Get(ctx, cacheKey, &pbSeedPeer); err == nil {
		log.Debugf("%s cache hit", cacheKey)
		return &pbSeedPeer, nil
	}

	// Cache miss.
	log.Debugf("%s cache miss", cacheKey)
	seedPeer := models.SeedPeer{}
	if err := s.db.WithContext(ctx).Preload("SeedPeerCluster").Preload("SeedPeerCluster.SchedulerClusters.Schedulers", &models.Scheduler{
		State: models.SchedulerStateActive,
	}).First(&seedPeer, &models.SeedPeer{
		HostName:          req.HostName,
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
			pbSchedulers = append(pbSchedulers, &managerv2.Scheduler{
				Id:       uint64(scheduler.ID),
				HostName: scheduler.HostName,
				Idc:      scheduler.IDC,
				Location: scheduler.Location,
				Ip:       scheduler.IP,
				Port:     scheduler.Port,
				State:    scheduler.State,
			})
		}
	}

	// Construct seed peer.
	pbSeedPeer = managerv2.SeedPeer{
		Id:                uint64(seedPeer.ID),
		Type:              seedPeer.Type,
		HostName:          seedPeer.HostName,
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
		log.Warn(err)
	}

	return &pbSeedPeer, nil
}

// Update SeedPeer configuration.
func (s *managerServerV2) UpdateSeedPeer(ctx context.Context, req *managerv2.UpdateSeedPeerRequest) (*managerv2.SeedPeer, error) {
	log := logger.WithHostnameAndIP(req.HostName, req.Ip)
	seedPeer := models.SeedPeer{}
	if err := s.db.WithContext(ctx).First(&seedPeer, models.SeedPeer{
		HostName:          req.HostName,
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
		cache.MakeSeedPeerCacheKey(seedPeer.SeedPeerClusterID, seedPeer.HostName, seedPeer.IP),
	); err != nil {
		log.Warn(err)
	}

	return &managerv2.SeedPeer{
		Id:                uint64(seedPeer.ID),
		HostName:          seedPeer.HostName,
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
		HostName:          req.HostName,
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
		HostName:          seedPeer.HostName,
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
	log := logger.WithHostnameAndIP(req.HostName, req.Ip)
	cacheKey := cache.MakeSchedulerCacheKey(uint(req.SchedulerClusterId), req.HostName, req.Ip)

	// Cache hit.
	var pbScheduler managerv2.Scheduler
	if err := s.cache.Get(ctx, cacheKey, &pbScheduler); err == nil {
		log.Debugf("%s cache hit", cacheKey)
		return &pbScheduler, nil
	}

	// Cache miss.
	log.Debugf("%s cache miss", cacheKey)
	scheduler := models.Scheduler{}
	if err := s.db.WithContext(ctx).Preload("SchedulerCluster").Preload("SchedulerCluster.SeedPeerClusters.SeedPeers", &models.SeedPeer{
		State: models.SeedPeerStateActive,
	}).First(&scheduler, &models.Scheduler{
		HostName:           req.HostName,
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
				HostName:          seedPeer.HostName,
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

	// Construct scheduler.
	pbScheduler = managerv2.Scheduler{
		Id:                 uint64(scheduler.ID),
		HostName:           scheduler.HostName,
		Idc:                scheduler.IDC,
		Location:           scheduler.Location,
		Ip:                 scheduler.IP,
		Port:               scheduler.Port,
		State:              scheduler.State,
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
		log.Warn(err)
	}

	return &pbScheduler, nil
}

// Update scheduler configuration.
func (s *managerServerV2) UpdateScheduler(ctx context.Context, req *managerv2.UpdateSchedulerRequest) (*managerv2.Scheduler, error) {
	log := logger.WithHostnameAndIP(req.HostName, req.Ip)
	scheduler := models.Scheduler{}
	if err := s.db.WithContext(ctx).First(&scheduler, models.Scheduler{
		HostName:           req.HostName,
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
		cache.MakeSchedulerCacheKey(scheduler.SchedulerClusterID, scheduler.HostName, scheduler.IP),
	); err != nil {
		log.Warn(err)
	}

	return &managerv2.Scheduler{
		Id:                 uint64(scheduler.ID),
		HostName:           scheduler.HostName,
		Idc:                scheduler.IDC,
		Location:           scheduler.Location,
		Ip:                 scheduler.IP,
		Port:               scheduler.Port,
		SchedulerClusterId: uint64(scheduler.SchedulerClusterID),
		State:              scheduler.State,
	}, nil
}

// Create scheduler and associate cluster.
func (s *managerServerV2) createScheduler(ctx context.Context, req *managerv2.UpdateSchedulerRequest) (*managerv2.Scheduler, error) {
	scheduler := models.Scheduler{
		HostName:           req.HostName,
		IDC:                req.Idc,
		Location:           req.Location,
		IP:                 req.Ip,
		Port:               req.Port,
		SchedulerClusterID: uint(req.SchedulerClusterId),
	}

	if err := s.db.WithContext(ctx).Create(&scheduler).Error; err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &managerv2.Scheduler{
		Id:                 uint64(scheduler.ID),
		HostName:           scheduler.HostName,
		Idc:                scheduler.IDC,
		Location:           scheduler.Location,
		Ip:                 scheduler.IP,
		Port:               scheduler.Port,
		State:              scheduler.State,
		SchedulerClusterId: uint64(scheduler.SchedulerClusterID),
	}, nil
}

// List acitve schedulers configuration.
func (s *managerServerV2) ListSchedulers(ctx context.Context, req *managerv2.ListSchedulersRequest) (*managerv2.ListSchedulersResponse, error) {
	log := logger.WithHostnameAndIP(req.HostName, req.Ip)
	log.Debugf("list schedulers, version %s, commit %s", req.Version, req.Commit)
	metrics.SearchSchedulerClusterCount.WithLabelValues(req.Version, req.Commit).Inc()

	// Count the number of the active peer.
	if s.config.Metrics.EnablePeerGauge && req.SourceType == managerv2.SourceType_PEER_SOURCE {
		peerCacheKey := fmt.Sprintf("%s-%s", req.HostName, req.Ip)
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
	cacheKey := cache.MakeSchedulersCacheKeyForPeer(req.HostName, req.Ip)

	if err := s.cache.Get(ctx, cacheKey, &pbListSchedulersResponse); err == nil {
		log.Debugf("%s cache hit", cacheKey)
		return &pbListSchedulersResponse, nil
	}

	// Cache miss.
	log.Debugf("%s cache miss", cacheKey)
	var schedulerClusters []models.SchedulerCluster
	if err := s.db.WithContext(ctx).Preload("SecurityGroup.SecurityRules").Preload("SeedPeerClusters.SeedPeers", "state = ?", "active").Preload("Schedulers", "state = ?", "active").Find(&schedulerClusters).Error; err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	log.Debugf("list scheduler clusters %v with hostInfo %#v", getSchedulerClusterNames(schedulerClusters), req.HostInfo)

	// Search optimal scheduler clusters.
	// If searcher can not found candidate scheduler cluster,
	// return all scheduler clusters.
	var (
		candidateSchedulerClusters []models.SchedulerCluster
		err                        error
	)
	candidateSchedulerClusters, err = s.searcher.FindSchedulerClusters(ctx, schedulerClusters, req.HostName, req.Ip, req.HostInfo)
	if err != nil {
		log.Error(err)
		metrics.SearchSchedulerClusterFailureCount.WithLabelValues(req.Version, req.Commit).Inc()
		candidateSchedulerClusters = schedulerClusters
	}
	log.Debugf("find matching scheduler cluster %v", getSchedulerClusterNames(schedulerClusters))

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
					HostName:          seedPeer.HostName,
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

		pbListSchedulersResponse.Schedulers = append(pbListSchedulersResponse.Schedulers, &managerv2.Scheduler{
			Id:                 uint64(scheduler.ID),
			HostName:           scheduler.HostName,
			Idc:                scheduler.IDC,
			Location:           scheduler.Location,
			Ip:                 scheduler.IP,
			Port:               scheduler.Port,
			State:              scheduler.State,
			SchedulerClusterId: uint64(scheduler.SchedulerClusterID),
			SeedPeers:          seedPeers,
		})
	}

	// Cache data.
	if err := s.cache.Once(&cachev8.Item{
		Ctx:   ctx,
		Key:   cacheKey,
		Value: &pbListSchedulersResponse,
		TTL:   s.cache.TTL,
	}); err != nil {
		log.Warn(err)
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

	log := logger.WithHostnameAndIP(req.HostName, req.Ip)
	var pbListBucketsResponse managerv2.ListBucketsResponse
	cacheKey := cache.MakeBucketCacheKey(s.objectStorageConfig.Name)

	// Cache hit.
	if err := s.cache.Get(ctx, cacheKey, &pbListBucketsResponse); err == nil {
		log.Debugf("%s cache hit", cacheKey)
		return &pbListBucketsResponse, nil
	}

	// Cache miss.
	log.Debugf("%s cache miss", cacheKey)
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
		log.Warn(err)
	}

	return &pbListBucketsResponse, nil
}

// List applications configuration.
func (s *managerServerV2) ListApplications(ctx context.Context, req *managerv2.ListApplicationsRequest) (*managerv2.ListApplicationsResponse, error) {
	log := logger.WithHostnameAndIP(req.HostName, req.Ip)

	// Cache hit.
	var pbListApplicationsResponse managerv2.ListApplicationsResponse
	cacheKey := cache.MakeApplicationsCacheKey()
	if err := s.cache.Get(ctx, cacheKey, &pbListApplicationsResponse); err == nil {
		log.Debugf("%s cache hit", cacheKey)
		return &pbListApplicationsResponse, nil
	}

	// Cache miss.
	log.Debugf("%s cache miss", cacheKey)
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
		log.Warn(err)
	}

	return &pbListApplicationsResponse, nil
}

// KeepAlive with manager.
func (s *managerServerV2) KeepAlive(stream managerv2.Manager_KeepAliveServer) error {
	req, err := stream.Recv()
	if err != nil {
		logger.Errorf("keepalive failed for the first time: %s", err.Error())
		return status.Error(codes.Internal, err.Error())
	}
	hostName := req.HostName
	ip := req.Ip
	sourceType := req.SourceType
	clusterID := uint(req.ClusterId)

	log := logger.WithKeepAlive(hostName, ip, sourceType.Enum().String(), req.ClusterId)
	log.Info("keepalive for the first time")

	// Initialize active scheduler.
	if sourceType == managerv2.SourceType_SCHEDULER_SOURCE {
		scheduler := models.Scheduler{}
		if err := s.db.First(&scheduler, models.Scheduler{
			HostName:           hostName,
			SchedulerClusterID: clusterID,
		}).Updates(models.Scheduler{
			State: models.SchedulerStateActive,
		}).Error; err != nil {
			return status.Error(codes.Internal, err.Error())
		}

		if err := s.cache.Delete(
			context.TODO(),
			cache.MakeSchedulerCacheKey(clusterID, hostName, ip),
		); err != nil {
			log.Warnf("refresh keepalive status failed: %s", err.Error())
		}
	}

	// Initialize active seed peer.
	if sourceType == managerv2.SourceType_SEED_PEER_SOURCE {
		seedPeer := models.SeedPeer{}
		if err := s.db.First(&seedPeer, models.SeedPeer{
			HostName:          hostName,
			SeedPeerClusterID: clusterID,
		}).Updates(models.SeedPeer{
			State: models.SeedPeerStateActive,
		}).Error; err != nil {
			return status.Error(codes.Internal, err.Error())
		}

		if err := s.cache.Delete(
			context.TODO(),
			cache.MakeSeedPeerCacheKey(clusterID, hostName, ip),
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
					HostName:           hostName,
					SchedulerClusterID: clusterID,
				}).Updates(models.Scheduler{
					State: models.SchedulerStateInactive,
				}).Error; err != nil {
					return status.Error(codes.Internal, err.Error())
				}

				if err := s.cache.Delete(
					context.TODO(),
					cache.MakeSchedulerCacheKey(clusterID, hostName, ip),
				); err != nil {
					log.Warnf("refresh keepalive status failed: %s", err.Error())
				}
			}

			// Inactive seed peer.
			if sourceType == managerv2.SourceType_SEED_PEER_SOURCE {
				seedPeer := models.SeedPeer{}
				if err := s.db.First(&seedPeer, models.SeedPeer{
					HostName:          hostName,
					SeedPeerClusterID: clusterID,
				}).Updates(models.SeedPeer{
					State: models.SeedPeerStateInactive,
				}).Error; err != nil {
					return status.Error(codes.Internal, err.Error())
				}

				if err := s.cache.Delete(
					context.TODO(),
					cache.MakeSeedPeerCacheKey(clusterID, hostName, ip),
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
