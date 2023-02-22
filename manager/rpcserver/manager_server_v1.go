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
	"time"

	cachev8 "github.com/go-redis/cache/v8"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"

	commonv1 "d7y.io/api/pkg/apis/common/v1"
	managerv1 "d7y.io/api/pkg/apis/manager/v1"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/manager/cache"
	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/manager/database"
	"d7y.io/dragonfly/v2/manager/metrics"
	"d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/manager/searcher"
	"d7y.io/dragonfly/v2/manager/types"
	pkgcache "d7y.io/dragonfly/v2/pkg/cache"
	"d7y.io/dragonfly/v2/pkg/objectstorage"
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

	// Peer memory cache.
	peerCache pkgcache.Cache

	// Searcher interface.
	searcher searcher.Searcher

	// Object storage interface.
	objectStorage objectstorage.ObjectStorage

	// Object storage configuration.
	objectStorageConfig *config.ObjectStorageConfig
}

// newManagerServerV1 returns v1 version of the manager server.
func newManagerServerV1(
	cfg *config.Config, database *database.Database, cache *cache.Cache, peerCache pkgcache.Cache, searcher searcher.Searcher,
	objectStorage objectstorage.ObjectStorage, objectStorageConfig *config.ObjectStorageConfig,
) managerv1.ManagerServer {
	return &managerServerV1{
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
func (s *managerServerV1) GetSeedPeer(ctx context.Context, req *managerv1.GetSeedPeerRequest) (*managerv1.SeedPeer, error) {
	log := logger.WithHostnameAndIP(req.HostName, req.Ip)
	cacheKey := cache.MakeSeedPeerCacheKey(uint(req.SeedPeerClusterId), req.HostName, req.Ip)

	// Cache hit.
	var pbSeedPeer managerv1.SeedPeer
	if err := s.cache.Get(ctx, cacheKey, &pbSeedPeer); err == nil {
		log.Debugf("%s cache hit", cacheKey)
		return &pbSeedPeer, nil
	}

	// Cache miss.
	log.Debugf("%s cache miss", cacheKey)
	seedPeer := model.SeedPeer{}
	if err := s.db.WithContext(ctx).Preload("SeedPeerCluster").Preload("SeedPeerCluster.SchedulerClusters.Schedulers", &model.Scheduler{
		State: model.SchedulerStateActive,
	}).First(&seedPeer, &model.SeedPeer{
		HostName:          req.HostName,
		SeedPeerClusterID: uint(req.SeedPeerClusterId),
	}).Error; err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
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
			pbSchedulers = append(pbSchedulers, &managerv1.Scheduler{
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
	pbSeedPeer = managerv1.SeedPeer{
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
		log.Warn(err)
	}

	return &pbSeedPeer, nil
}

// Update SeedPeer configuration.
func (s *managerServerV1) UpdateSeedPeer(ctx context.Context, req *managerv1.UpdateSeedPeerRequest) (*managerv1.SeedPeer, error) {
	log := logger.WithHostnameAndIP(req.HostName, req.Ip)
	seedPeer := model.SeedPeer{}
	if err := s.db.WithContext(ctx).First(&seedPeer, model.SeedPeer{
		HostName:          req.HostName,
		SeedPeerClusterID: uint(req.SeedPeerClusterId),
	}).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return s.createSeedPeer(ctx, req)
		}
		return nil, status.Error(codes.Unknown, err.Error())
	}

	if err := s.db.WithContext(ctx).Model(&seedPeer).Updates(model.SeedPeer{
		Type:              req.Type,
		IDC:               req.Idc,
		Location:          req.Location,
		IP:                req.Ip,
		Port:              req.Port,
		DownloadPort:      req.DownloadPort,
		ObjectStoragePort: req.ObjectStoragePort,
		SeedPeerClusterID: uint(req.SeedPeerClusterId),
	}).Error; err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
	}

	if err := s.cache.Delete(
		ctx,
		cache.MakeSeedPeerCacheKey(seedPeer.SeedPeerClusterID, seedPeer.HostName, seedPeer.IP),
	); err != nil {
		log.Warn(err)
	}

	return &managerv1.SeedPeer{
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
func (s *managerServerV1) createSeedPeer(ctx context.Context, req *managerv1.UpdateSeedPeerRequest) (*managerv1.SeedPeer, error) {
	seedPeer := model.SeedPeer{
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
		return nil, status.Error(codes.Unknown, err.Error())
	}

	return &managerv1.SeedPeer{
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
func (s *managerServerV1) GetScheduler(ctx context.Context, req *managerv1.GetSchedulerRequest) (*managerv1.Scheduler, error) {
	log := logger.WithHostnameAndIP(req.HostName, req.Ip)
	cacheKey := cache.MakeSchedulerCacheKey(uint(req.SchedulerClusterId), req.HostName, req.Ip)

	// Cache hit.
	var pbScheduler managerv1.Scheduler
	if err := s.cache.Get(ctx, cacheKey, &pbScheduler); err == nil {
		log.Debugf("%s cache hit", cacheKey)
		return &pbScheduler, nil
	}

	// Cache miss.
	log.Debugf("%s cache miss", cacheKey)
	scheduler := model.Scheduler{}
	if err := s.db.WithContext(ctx).Preload("SchedulerCluster").Preload("SchedulerCluster.SeedPeerClusters.SeedPeers", &model.SeedPeer{
		State: model.SeedPeerStateActive,
	}).First(&scheduler, &model.Scheduler{
		HostName:           req.HostName,
		SchedulerClusterID: uint(req.SchedulerClusterId),
	}).Error; err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
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
				SeedPeerCluster: &managerv1.SeedPeerCluster{
					Id:     uint64(seedPeerCluster.ID),
					Name:   seedPeerCluster.Name,
					Bio:    seedPeerCluster.BIO,
					Config: seedPeerClusterConfig,
				},
			})
		}
	}

	// Construct scheduler.
	pbScheduler = managerv1.Scheduler{
		Id:                 uint64(scheduler.ID),
		HostName:           scheduler.HostName,
		Idc:                scheduler.IDC,
		Location:           scheduler.Location,
		Ip:                 scheduler.IP,
		Port:               scheduler.Port,
		State:              scheduler.State,
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
		log.Warn(err)
	}

	return &pbScheduler, nil
}

// Update scheduler configuration.
func (s *managerServerV1) UpdateScheduler(ctx context.Context, req *managerv1.UpdateSchedulerRequest) (*managerv1.Scheduler, error) {
	log := logger.WithHostnameAndIP(req.HostName, req.Ip)
	scheduler := model.Scheduler{}
	if err := s.db.WithContext(ctx).First(&scheduler, model.Scheduler{
		HostName:           req.HostName,
		SchedulerClusterID: uint(req.SchedulerClusterId),
	}).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return s.createScheduler(ctx, req)
		}
		return nil, status.Error(codes.Unknown, err.Error())
	}

	if err := s.db.WithContext(ctx).Model(&scheduler).Updates(model.Scheduler{
		IDC:                req.Idc,
		Location:           req.Location,
		IP:                 req.Ip,
		Port:               req.Port,
		SchedulerClusterID: uint(req.SchedulerClusterId),
	}).Error; err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
	}

	if err := s.cache.Delete(
		ctx,
		cache.MakeSchedulerCacheKey(scheduler.SchedulerClusterID, scheduler.HostName, scheduler.IP),
	); err != nil {
		log.Warn(err)
	}

	return &managerv1.Scheduler{
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
func (s *managerServerV1) createScheduler(ctx context.Context, req *managerv1.UpdateSchedulerRequest) (*managerv1.Scheduler, error) {
	scheduler := model.Scheduler{
		HostName:           req.HostName,
		IDC:                req.Idc,
		Location:           req.Location,
		IP:                 req.Ip,
		Port:               req.Port,
		SchedulerClusterID: uint(req.SchedulerClusterId),
	}

	if err := s.db.WithContext(ctx).Create(&scheduler).Error; err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
	}

	return &managerv1.Scheduler{
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
func (s *managerServerV1) ListSchedulers(ctx context.Context, req *managerv1.ListSchedulersRequest) (*managerv1.ListSchedulersResponse, error) {
	log := logger.WithHostnameAndIP(req.HostName, req.Ip)
	log.Debugf("list schedulers, version %s, commit %s", req.Version, req.Commit)

	// Count the number of the active peer.
	if s.config.Metrics.EnablePeerGauge && req.SourceType == managerv1.SourceType_PEER_SOURCE {
		peerCacheKey := fmt.Sprintf("%s-%s", req.HostName, req.Ip)
		if data, _, found := s.peerCache.GetWithExpiration(peerCacheKey); !found {
			metrics.PeerGauge.WithLabelValues(req.Version, req.Commit).Inc()
		} else if cache, ok := data.(*managerv1.ListSchedulersRequest); ok && (cache.Version != req.Version || cache.Commit != req.Commit) {
			metrics.PeerGauge.WithLabelValues(cache.Version, cache.Commit).Dec()
			metrics.PeerGauge.WithLabelValues(req.Version, req.Commit).Inc()
		}

		s.peerCache.SetDefault(peerCacheKey, req)
	}

	// Cache hit.
	var pbListSchedulersResponse managerv1.ListSchedulersResponse
	cacheKey := cache.MakeSchedulersCacheKeyForPeer(req.HostName, req.Ip)

	if err := s.cache.Get(ctx, cacheKey, &pbListSchedulersResponse); err == nil {
		log.Debugf("%s cache hit", cacheKey)
		return &pbListSchedulersResponse, nil
	}

	// Cache miss.
	log.Debugf("%s cache miss", cacheKey)
	var schedulerClusters []model.SchedulerCluster
	if err := s.db.WithContext(ctx).Preload("SecurityGroup.SecurityRules").Preload("SeedPeerClusters.SeedPeers", "state = ?", "active").Preload("Schedulers", "state = ?", "active").Find(&schedulerClusters).Error; err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
	}

	log.Debugf("list scheduler clusters %v with hostInfo %#v", getSchedulerClusterNames(schedulerClusters), req.HostInfo)

	// Search optimal scheduler clusters.
	// If searcher can not found candidate scheduler cluster,
	// return all scheduler clusters.
	var (
		candidateSchedulerClusters []model.SchedulerCluster
		err                        error
	)
	candidateSchedulerClusters, err = s.searcher.FindSchedulerClusters(ctx, schedulerClusters, req.HostName, req.Ip, req.HostInfo)
	if err != nil {
		log.Error(err)
		metrics.SearchSchedulerClusterFailureCount.WithLabelValues(req.Version, req.Commit).Inc()
		candidateSchedulerClusters = schedulerClusters
	}
	log.Debugf("find matching scheduler cluster %v", getSchedulerClusterNames(schedulerClusters))

	schedulers := []model.Scheduler{}
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

		pbListSchedulersResponse.Schedulers = append(pbListSchedulersResponse.Schedulers, &managerv1.Scheduler{
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
func (s *managerServerV1) GetObjectStorage(ctx context.Context, req *managerv1.GetObjectStorageRequest) (*managerv1.ObjectStorage, error) {
	if !s.objectStorageConfig.Enable {
		return nil, status.Error(codes.NotFound, "object storage is disabled")
	}

	return &managerv1.ObjectStorage{
		Name:             s.objectStorageConfig.Name,
		Region:           s.objectStorageConfig.Region,
		Endpoint:         s.objectStorageConfig.Endpoint,
		AccessKey:        s.objectStorageConfig.AccessKey,
		SecretKey:        s.objectStorageConfig.SecretKey,
		S3ForcePathStyle: s.objectStorageConfig.S3ForcePathStyle,
	}, nil
}

// List buckets configuration.
func (s *managerServerV1) ListBuckets(ctx context.Context, req *managerv1.ListBucketsRequest) (*managerv1.ListBucketsResponse, error) {
	if !s.objectStorageConfig.Enable {
		return nil, status.Error(codes.NotFound, "object storage is disabled")
	}

	log := logger.WithHostnameAndIP(req.HostName, req.Ip)
	var pbListBucketsResponse managerv1.ListBucketsResponse
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
		return nil, status.Error(codes.Unknown, err.Error())
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
		log.Warn(err)
	}

	return &pbListBucketsResponse, nil
}

// List models information.
func (s *managerServerV1) ListModels(ctx context.Context, req *managerv1.ListModelsRequest) (*managerv1.ListModelsResponse, error) {
	scheduler := model.Scheduler{}
	if err := s.db.WithContext(ctx).First(&scheduler, req.SchedulerId).Error; err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
	}

	models := []*managerv1.Model{}
	iter := s.rdb.Scan(ctx, 0, cache.MakeModelKey(scheduler.SchedulerClusterID, scheduler.HostName, scheduler.IP, "*"), 0).Iterator()
	for iter.Next(ctx) {
		var model types.Model
		if err := s.rdb.Get(ctx, iter.Val()).Scan(&model); err != nil {
			return nil, status.Error(codes.Unknown, err.Error())
		}

		models = append(models, &managerv1.Model{
			ModelId:     model.ID,
			Name:        model.Name,
			VersionId:   model.VersionID,
			SchedulerId: uint64(model.SchedulerID),
			HostName:    model.Hostname,
			Ip:          model.IP,
			CreatedAt:   timestamppb.New(model.CreatedAt),
			UpdatedAt:   timestamppb.New(model.UpdatedAt),
		})
	}

	return &managerv1.ListModelsResponse{
		Models: models,
	}, nil
}

// Get model information.
func (s *managerServerV1) GetModel(ctx context.Context, req *managerv1.GetModelRequest) (*managerv1.Model, error) {
	scheduler := model.Scheduler{}
	if err := s.db.WithContext(ctx).First(&scheduler, req.SchedulerId).Error; err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
	}

	var model types.Model
	if err := s.rdb.Get(ctx, cache.MakeModelKey(scheduler.SchedulerClusterID, scheduler.HostName, scheduler.IP, req.ModelId)).Scan(&model); err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
	}

	return &managerv1.Model{
		ModelId:     model.ID,
		Name:        model.Name,
		VersionId:   model.VersionID,
		SchedulerId: uint64(model.SchedulerID),
		HostName:    model.Hostname,
		Ip:          model.IP,
		CreatedAt:   timestamppb.New(model.CreatedAt),
		UpdatedAt:   timestamppb.New(model.UpdatedAt),
	}, nil
}

// Create model information.
func (s *managerServerV1) CreateModel(ctx context.Context, req *managerv1.CreateModelRequest) (*managerv1.Model, error) {
	scheduler := model.Scheduler{}
	if err := s.db.WithContext(ctx).First(&scheduler, req.SchedulerId).Error; err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
	}

	model := types.Model{
		ID:          req.ModelId,
		Name:        req.Name,
		VersionID:   req.VersionId,
		SchedulerID: uint(req.SchedulerId),
		Hostname:    req.HostName,
		IP:          req.Ip,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}

	if _, err := s.rdb.Set(ctx, cache.MakeModelKey(scheduler.SchedulerClusterID, scheduler.HostName, scheduler.IP, model.ID), &model, 0).Result(); err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
	}

	return &managerv1.Model{
		ModelId:     model.ID,
		Name:        model.Name,
		VersionId:   model.VersionID,
		SchedulerId: uint64(model.SchedulerID),
		HostName:    model.Hostname,
		Ip:          model.IP,
		CreatedAt:   timestamppb.New(model.CreatedAt),
		UpdatedAt:   timestamppb.New(model.UpdatedAt),
	}, nil
}

// Update model information.
func (s *managerServerV1) UpdateModel(ctx context.Context, req *managerv1.UpdateModelRequest) (*managerv1.Model, error) {
	scheduler := model.Scheduler{}
	if err := s.db.WithContext(ctx).First(&scheduler, req.SchedulerId).Error; err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
	}

	model, err := s.GetModel(ctx, &managerv1.GetModelRequest{
		SchedulerId: req.SchedulerId,
		ModelId:     req.ModelId,
	})
	if err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
	}

	model.VersionId = req.VersionId
	model.UpdatedAt = timestamppb.New(time.Now())

	if _, err := s.rdb.Set(ctx, cache.MakeModelKey(scheduler.SchedulerClusterID, scheduler.HostName, scheduler.IP, req.ModelId), types.Model{
		ID:          model.ModelId,
		Name:        model.Name,
		VersionID:   model.VersionId,
		SchedulerID: uint(model.SchedulerId),
		Hostname:    model.HostName,
		IP:          model.Ip,
		CreatedAt:   model.CreatedAt.AsTime(),
		UpdatedAt:   model.UpdatedAt.AsTime(),
	}, 0).Result(); err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
	}

	return model, nil
}

// Delete model information.
func (s *managerServerV1) DeleteModel(ctx context.Context, req *managerv1.DeleteModelRequest) (*emptypb.Empty, error) {
	if _, err := s.GetModel(ctx, &managerv1.GetModelRequest{
		SchedulerId: req.SchedulerId,
		ModelId:     req.ModelId,
	}); err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
	}

	scheduler := model.Scheduler{}
	if err := s.db.WithContext(ctx).First(&scheduler, req.SchedulerId).Error; err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
	}

	if _, err := s.rdb.Del(ctx, cache.MakeModelKey(scheduler.SchedulerClusterID, scheduler.HostName, scheduler.IP, req.ModelId)).Result(); err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
	}

	return nil, nil
}

// List model versions information.
func (s *managerServerV1) ListModelVersions(ctx context.Context, req *managerv1.ListModelVersionsRequest) (*managerv1.ListModelVersionsResponse, error) {
	scheduler := model.Scheduler{}
	if err := s.db.WithContext(ctx).First(&scheduler, req.SchedulerId).Error; err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
	}

	modelVersions := []*managerv1.ModelVersion{}
	iter := s.rdb.Scan(ctx, 0, cache.MakeModelVersionKey(scheduler.SchedulerClusterID, scheduler.HostName, scheduler.IP, req.ModelId, "*"), 0).Iterator()
	for iter.Next(ctx) {
		var modelVersion types.ModelVersion
		if err := s.rdb.Get(ctx, iter.Val()).Scan(&modelVersion); err != nil {
			return nil, status.Error(codes.Unknown, err.Error())
		}

		modelVersions = append(modelVersions, &managerv1.ModelVersion{
			VersionId: modelVersion.ID,
			Data:      modelVersion.Data,
			Mae:       modelVersion.MAE,
			Mse:       modelVersion.MSE,
			Rmse:      modelVersion.RMSE,
			R2:        modelVersion.R2,
			CreatedAt: timestamppb.New(modelVersion.CreatedAt),
			UpdatedAt: timestamppb.New(modelVersion.UpdatedAt),
		})
	}

	return &managerv1.ListModelVersionsResponse{
		ModelVersions: modelVersions,
	}, nil
}

// Get model version information.
func (s *managerServerV1) GetModelVersion(ctx context.Context, req *managerv1.GetModelVersionRequest) (*managerv1.ModelVersion, error) {
	scheduler := model.Scheduler{}
	if err := s.db.WithContext(ctx).First(&scheduler, req.SchedulerId).Error; err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
	}

	var modelVersion types.ModelVersion
	if err := s.rdb.Get(ctx, cache.MakeModelVersionKey(scheduler.SchedulerClusterID, scheduler.HostName, scheduler.IP, req.ModelId, req.VersionId)).Scan(&modelVersion); err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
	}

	return &managerv1.ModelVersion{
		VersionId: modelVersion.ID,
		Data:      modelVersion.Data,
		Mae:       modelVersion.MAE,
		Mse:       modelVersion.MSE,
		Rmse:      modelVersion.RMSE,
		R2:        modelVersion.R2,
		CreatedAt: timestamppb.New(modelVersion.CreatedAt),
		UpdatedAt: timestamppb.New(modelVersion.UpdatedAt),
	}, nil
}

// Create model version information.
func (s *managerServerV1) CreateModelVersion(ctx context.Context, req *managerv1.CreateModelVersionRequest) (*managerv1.ModelVersion, error) {
	scheduler := model.Scheduler{}
	if err := s.db.WithContext(ctx).First(&scheduler, req.SchedulerId).Error; err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
	}

	modelVersion := types.ModelVersion{
		ID:        uuid.New().String(),
		Data:      req.Data,
		MAE:       req.Mae,
		MSE:       req.Mse,
		RMSE:      req.Rmse,
		R2:        req.R2,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	if _, err := s.rdb.Set(ctx, cache.MakeModelVersionKey(scheduler.SchedulerClusterID, scheduler.HostName, scheduler.IP, req.ModelId, modelVersion.ID), &modelVersion, 0).Result(); err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
	}

	return &managerv1.ModelVersion{
		VersionId: modelVersion.ID,
		Data:      modelVersion.Data,
		Mae:       modelVersion.MAE,
		Mse:       modelVersion.MSE,
		Rmse:      modelVersion.RMSE,
		R2:        modelVersion.R2,
		CreatedAt: timestamppb.New(modelVersion.CreatedAt),
		UpdatedAt: timestamppb.New(modelVersion.UpdatedAt),
	}, nil
}

// Update model version information.
func (s *managerServerV1) UpdateModelVersion(ctx context.Context, req *managerv1.UpdateModelVersionRequest) (*managerv1.ModelVersion, error) {
	scheduler := model.Scheduler{}
	if err := s.db.WithContext(ctx).First(&scheduler, req.SchedulerId).Error; err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
	}

	modelVersion, err := s.GetModelVersion(ctx, &managerv1.GetModelVersionRequest{
		SchedulerId: req.SchedulerId,
		ModelId:     req.ModelId,
		VersionId:   req.VersionId,
	})
	if err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
	}

	if req.Mae > 0 {
		modelVersion.Mae = req.Mae
	}

	if req.Mse > 0 {
		modelVersion.Mse = req.Mse
	}

	if req.Rmse > 0 {
		modelVersion.Rmse = req.Rmse
	}

	if req.R2 > 0 {
		modelVersion.R2 = req.R2
	}

	if len(req.Data) > 0 {
		modelVersion.Data = req.Data
	}

	modelVersion.UpdatedAt = timestamppb.New(time.Now())

	if _, err := s.rdb.Set(ctx, cache.MakeModelVersionKey(scheduler.SchedulerClusterID, scheduler.HostName, scheduler.IP, req.ModelId, modelVersion.VersionId), &types.ModelVersion{
		ID:        modelVersion.VersionId,
		Data:      modelVersion.Data,
		MAE:       modelVersion.Mae,
		MSE:       modelVersion.Mse,
		RMSE:      modelVersion.Rmse,
		R2:        modelVersion.R2,
		CreatedAt: modelVersion.CreatedAt.AsTime(),
		UpdatedAt: modelVersion.UpdatedAt.AsTime(),
	}, 0).Result(); err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
	}

	return modelVersion, nil
}

// Delete model version information.
func (s *managerServerV1) DeleteModelVersion(ctx context.Context, req *managerv1.DeleteModelVersionRequest) (*emptypb.Empty, error) {
	if _, err := s.GetModelVersion(ctx, &managerv1.GetModelVersionRequest{
		SchedulerId: req.SchedulerId,
		ModelId:     req.ModelId,
		VersionId:   req.VersionId,
	}); err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
	}

	scheduler := model.Scheduler{}
	if err := s.db.WithContext(ctx).First(&scheduler, req.SchedulerId).Error; err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
	}

	if _, err := s.rdb.Del(ctx, cache.MakeModelVersionKey(scheduler.SchedulerClusterID, scheduler.HostName, scheduler.IP, req.ModelId, req.VersionId)).Result(); err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
	}

	return nil, nil
}

// List applications configuration.
func (s *managerServerV1) ListApplications(ctx context.Context, req *managerv1.ListApplicationsRequest) (*managerv1.ListApplicationsResponse, error) {
	log := logger.WithHostnameAndIP(req.HostName, req.Ip)

	// Cache hit.
	var pbListApplicationsResponse managerv1.ListApplicationsResponse
	cacheKey := cache.MakeApplicationsCacheKey()
	if err := s.cache.Get(ctx, cacheKey, &pbListApplicationsResponse); err == nil {
		log.Debugf("%s cache hit", cacheKey)
		return &pbListApplicationsResponse, nil
	}

	// Cache miss.
	log.Debugf("%s cache miss", cacheKey)
	var applications []model.Application
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
		log.Warn(err)
	}

	return &pbListApplicationsResponse, nil
}

// KeepAlive with manager.
func (s *managerServerV1) KeepAlive(stream managerv1.Manager_KeepAliveServer) error {
	req, err := stream.Recv()
	if err != nil {
		logger.Errorf("keepalive failed for the first time: %s", err.Error())
		return status.Error(codes.Unknown, err.Error())
	}
	hostName := req.HostName
	ip := req.Ip
	sourceType := req.SourceType
	clusterID := uint(req.ClusterId)

	log := logger.WithKeepAlive(hostName, ip, sourceType.Enum().String(), req.ClusterId)
	log.Info("keepalive for the first time")

	// Initialize active scheduler.
	if sourceType == managerv1.SourceType_SCHEDULER_SOURCE {
		scheduler := model.Scheduler{}
		if err := s.db.First(&scheduler, model.Scheduler{
			HostName:           hostName,
			SchedulerClusterID: clusterID,
		}).Updates(model.Scheduler{
			State: model.SchedulerStateActive,
		}).Error; err != nil {
			return status.Error(codes.Unknown, err.Error())
		}

		if err := s.cache.Delete(
			context.TODO(),
			cache.MakeSchedulerCacheKey(clusterID, hostName, ip),
		); err != nil {
			log.Warnf("refresh keepalive status failed: %s", err.Error())
		}
	}

	// Initialize active seed peer.
	if sourceType == managerv1.SourceType_SEED_PEER_SOURCE {
		seedPeer := model.SeedPeer{}
		if err := s.db.First(&seedPeer, model.SeedPeer{
			HostName:          hostName,
			SeedPeerClusterID: clusterID,
		}).Updates(model.SeedPeer{
			State: model.SeedPeerStateActive,
		}).Error; err != nil {
			return status.Error(codes.Unknown, err.Error())
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
			if sourceType == managerv1.SourceType_SCHEDULER_SOURCE {
				scheduler := model.Scheduler{}
				if err := s.db.First(&scheduler, model.Scheduler{
					HostName:           hostName,
					SchedulerClusterID: clusterID,
				}).Updates(model.Scheduler{
					State: model.SchedulerStateInactive,
				}).Error; err != nil {
					return status.Error(codes.Unknown, err.Error())
				}

				if err := s.cache.Delete(
					context.TODO(),
					cache.MakeSchedulerCacheKey(clusterID, hostName, ip),
				); err != nil {
					log.Warnf("refresh keepalive status failed: %s", err.Error())
				}
			}

			// Inactive seed peer.
			if sourceType == managerv1.SourceType_SEED_PEER_SOURCE {
				seedPeer := model.SeedPeer{}
				if err := s.db.First(&seedPeer, model.SeedPeer{
					HostName:          hostName,
					SeedPeerClusterID: clusterID,
				}).Updates(model.SeedPeer{
					State: model.SeedPeerStateInactive,
				}).Error; err != nil {
					return status.Error(codes.Unknown, err.Error())
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
