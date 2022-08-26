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

package rpcserver

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	"time"

	cachev8 "github.com/go-redis/cache/v8"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"

	managerv1 "d7y.io/api/pkg/apis/manager/v1"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/manager/cache"
	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/manager/database"
	"d7y.io/dragonfly/v2/manager/metrics"
	"d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/manager/searcher"
	"d7y.io/dragonfly/v2/manager/types"
	"d7y.io/dragonfly/v2/pkg/objectstorage"
	managerserver "d7y.io/dragonfly/v2/pkg/rpc/manager/server"
)

// Server is grpc server.
type Server struct {
	// Manager configuration.
	config *config.Config

	// GORM instance.
	db *gorm.DB

	// Redis client instance.
	rdb *redis.Client

	// Cache instance.
	cache *cache.Cache

	// Searcher interface.
	searcher searcher.Searcher

	// Object storage interface.
	objectStorage objectstorage.ObjectStorage

	// Object storage configuration.
	objectStorageConfig *config.ObjectStorageConfig

	// serverOptions is server options of grpc.
	serverOptions []grpc.ServerOption

	// cert certificates to sign certificates.
	cert *tls.Certificate

	// x509Cert certificates to sign certificates.
	x509Cert *x509.Certificate

	// certChain is PEM-encoded certificate chain.
	certChain []string
}

// Option is a functional option for rpc server.
type Option func(s *Server) error

// WithCertificate set the root tls certificate, x509 certificate and PEM-encoded certificate chain.
func WithCertificate(cert *tls.Certificate) Option {
	return func(s *Server) error {
		s.cert = cert

		// Parse x509 certificate from tls certificate.
		var err error
		s.x509Cert, err = x509.ParseCertificate(cert.Certificate[0])
		if err != nil {
			return err
		}

		// Initialize PEM-encoded certificate chain from tls certificate.
		for _, cert := range cert.Certificate {
			var certChainPEM bytes.Buffer
			if err = pem.Encode(&certChainPEM, &pem.Block{Type: "CERTIFICATE", Bytes: cert}); err != nil {
				return err
			}

			s.certChain = append(s.certChain, certChainPEM.String())
		}

		return nil
	}
}

// WithGRPCServerOptions set the server options of grpc.
func WithGRPCServerOptions(opts []grpc.ServerOption) Option {
	return func(s *Server) error {
		s.serverOptions = opts
		return nil
	}
}

// New returns a new manager server from the given options.
func New(
	cfg *config.Config, database *database.Database, cache *cache.Cache, searcher searcher.Searcher,
	objectStorage objectstorage.ObjectStorage, objectStorageConfig *config.ObjectStorageConfig, opts ...Option,
) (*Server, *grpc.Server, error) {
	s := &Server{
		config:              cfg,
		db:                  database.DB,
		rdb:                 database.RDB,
		cache:               cache,
		searcher:            searcher,
		objectStorage:       objectStorage,
		objectStorageConfig: objectStorageConfig,
	}

	for _, opt := range opts {
		if err := opt(s); err != nil {
			return nil, nil, err
		}
	}

	return s, managerserver.New(s, s.serverOptions...), nil
}

// Get SeedPeer and SeedPeer cluster configuration.
func (s *Server) GetSeedPeer(ctx context.Context, req *managerv1.GetSeedPeerRequest) (*managerv1.SeedPeer, error) {
	var pbSeedPeer managerv1.SeedPeer
	cacheKey := cache.MakeSeedPeerCacheKey(uint(req.SeedPeerClusterId), req.HostName, req.Ip)

	// Cache hit.
	if err := s.cache.Get(ctx, cacheKey, &pbSeedPeer); err == nil {
		logger.Infof("%s cache hit", cacheKey)
		return &pbSeedPeer, nil
	}

	// Cache miss.
	logger.Infof("%s cache miss", cacheKey)
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
				Id:          uint64(scheduler.ID),
				HostName:    scheduler.HostName,
				Idc:         scheduler.IDC,
				NetTopology: scheduler.NetTopology,
				Location:    scheduler.Location,
				Ip:          scheduler.IP,
				Port:        scheduler.Port,
				State:       scheduler.State,
			})
		}
	}

	// Construct seed peer.
	pbSeedPeer = managerv1.SeedPeer{
		Id:                uint64(seedPeer.ID),
		Type:              seedPeer.Type,
		HostName:          seedPeer.HostName,
		Idc:               seedPeer.IDC,
		NetTopology:       seedPeer.NetTopology,
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
		logger.Warnf("storage cache failed: %v", err)
	}

	return &pbSeedPeer, nil
}

// Update SeedPeer configuration.
func (s *Server) UpdateSeedPeer(ctx context.Context, req *managerv1.UpdateSeedPeerRequest) (*managerv1.SeedPeer, error) {
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
		NetTopology:       req.NetTopology,
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
		logger.Warnf("%s refresh keepalive status failed in seed peer cluster %d", seedPeer.HostName, seedPeer.SeedPeerClusterID)
	}

	return &managerv1.SeedPeer{
		Id:                uint64(seedPeer.ID),
		HostName:          seedPeer.HostName,
		Type:              seedPeer.Type,
		Idc:               seedPeer.IDC,
		NetTopology:       seedPeer.NetTopology,
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
func (s *Server) createSeedPeer(ctx context.Context, req *managerv1.UpdateSeedPeerRequest) (*managerv1.SeedPeer, error) {
	seedPeer := model.SeedPeer{
		HostName:          req.HostName,
		Type:              req.Type,
		IDC:               req.Idc,
		NetTopology:       req.NetTopology,
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
		NetTopology:       seedPeer.NetTopology,
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
func (s *Server) GetScheduler(ctx context.Context, req *managerv1.GetSchedulerRequest) (*managerv1.Scheduler, error) {
	var pbScheduler managerv1.Scheduler
	cacheKey := cache.MakeSchedulerCacheKey(uint(req.SchedulerClusterId), req.HostName, req.Ip)

	// Cache hit.
	if err := s.cache.Get(ctx, cacheKey, &pbScheduler); err == nil {
		logger.Infof("%s cache hit", cacheKey)
		return &pbScheduler, nil
	}

	// Cache miss.
	logger.Infof("%s cache miss", cacheKey)
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
				NetTopology:       seedPeer.NetTopology,
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
		NetTopology:        scheduler.NetTopology,
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
		logger.Warnf("cache storage failed: %v", err)
	}

	return &pbScheduler, nil
}

// Update scheduler configuration.
func (s *Server) UpdateScheduler(ctx context.Context, req *managerv1.UpdateSchedulerRequest) (*managerv1.Scheduler, error) {
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
		NetTopology:        req.NetTopology,
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
		logger.Warnf("%s refresh keepalive status failed in scheduler cluster %d", scheduler.HostName, scheduler.SchedulerClusterID)
	}

	return &managerv1.Scheduler{
		Id:                 uint64(scheduler.ID),
		HostName:           scheduler.HostName,
		Idc:                scheduler.IDC,
		NetTopology:        scheduler.NetTopology,
		Location:           scheduler.Location,
		Ip:                 scheduler.IP,
		Port:               scheduler.Port,
		SchedulerClusterId: uint64(scheduler.SchedulerClusterID),
		State:              scheduler.State,
	}, nil
}

// Create scheduler and associate cluster.
func (s *Server) createScheduler(ctx context.Context, req *managerv1.UpdateSchedulerRequest) (*managerv1.Scheduler, error) {
	scheduler := model.Scheduler{
		HostName:           req.HostName,
		IDC:                req.Idc,
		NetTopology:        req.NetTopology,
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
		NetTopology:        scheduler.NetTopology,
		Location:           scheduler.Location,
		Ip:                 scheduler.IP,
		Port:               scheduler.Port,
		State:              scheduler.State,
		SchedulerClusterId: uint64(scheduler.SchedulerClusterID),
	}, nil
}

// List acitve schedulers configuration.
func (s *Server) ListSchedulers(ctx context.Context, req *managerv1.ListSchedulersRequest) (*managerv1.ListSchedulersResponse, error) {
	log := logger.WithHostnameAndIP(req.HostName, req.Ip)

	// Count the number of the active peer.
	if s.config.Metrics.EnablePeerGauge && req.SourceType == managerv1.SourceType_PEER_SOURCE {
		count, err := s.getPeerCount(ctx, req)
		if err != nil {
			log.Warnf("get peer count failed: %s", err.Error())
		} else {
			metrics.PeerGauge.WithLabelValues(req.Version, req.Commit).Set(float64(count))
		}
	}

	var pbListSchedulersResponse managerv1.ListSchedulersResponse
	cacheKey := cache.MakeSchedulersCacheKeyForPeer(req.HostName, req.Ip)

	// Cache hit.
	if err := s.cache.Get(ctx, cacheKey, &pbListSchedulersResponse); err == nil {
		log.Infof("%s cache hit", cacheKey)
		return &pbListSchedulersResponse, nil
	}

	// Cache miss.
	log.Infof("%s cache miss", cacheKey)
	var schedulerClusters []model.SchedulerCluster
	if err := s.db.WithContext(ctx).Preload("SecurityGroup.SecurityRules").Preload("SeedPeerClusters.SeedPeers", "state = ?", "active").Preload("Schedulers", "state = ?", "active").Find(&schedulerClusters).Error; err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
	}

	// Search optimal scheduler clusters.
	log.Infof("list scheduler clusters %v with hostInfo %#v", getSchedulerClusterNames(schedulerClusters), req.HostInfo)
	schedulerClusters, err := s.searcher.FindSchedulerClusters(ctx, schedulerClusters, req)
	if err != nil {
		log.Error(err)
		return nil, status.Error(codes.NotFound, "scheduler cluster not found")
	}
	log.Infof("find matching scheduler cluster %v", getSchedulerClusterNames(schedulerClusters))

	schedulers := []model.Scheduler{}
	for _, schedulerCluster := range schedulerClusters {
		for _, scheduler := range schedulerCluster.Schedulers {
			scheduler.SchedulerCluster = schedulerCluster
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
					NetTopology:       seedPeer.NetTopology,
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
			NetTopology:        scheduler.NetTopology,
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
		log.Warnf("storage cache failed: %v", err)
	}

	return &pbListSchedulersResponse, nil
}

// Get the number of active peers
func (s *Server) getPeerCount(ctx context.Context, req *managerv1.ListSchedulersRequest) (int, error) {
	cacheKey := cache.MakePeerCacheKey(req.HostName, req.Ip)
	if err := s.rdb.Set(ctx, cacheKey, types.Peer{
		ID:       cacheKey,
		Hostname: req.HostName,
		IP:       req.Ip,
	}, cache.PeerCacheTTL).Err(); err != nil {
		return 0, err
	}

	val, err := s.rdb.Keys(ctx, cache.MakeCacheKey(cache.PeerNamespace, "*")).Result()
	if err != nil {
		return 0, err
	}

	return len(val), nil
}

// Get object storage configuration.
func (s *Server) GetObjectStorage(ctx context.Context, req *managerv1.GetObjectStorageRequest) (*managerv1.ObjectStorage, error) {
	log := logger.WithHostnameAndIP(req.HostName, req.Ip)

	if !s.objectStorageConfig.Enable {
		msg := "object storage is disabled"
		log.Debug(msg)
		return nil, status.Error(codes.NotFound, msg)
	}

	return &managerv1.ObjectStorage{
		Name:      s.objectStorageConfig.Name,
		Region:    s.objectStorageConfig.Region,
		Endpoint:  s.objectStorageConfig.Endpoint,
		AccessKey: s.objectStorageConfig.AccessKey,
		SecretKey: s.objectStorageConfig.SecretKey,
	}, nil
}

// List buckets configuration.
func (s *Server) ListBuckets(ctx context.Context, req *managerv1.ListBucketsRequest) (*managerv1.ListBucketsResponse, error) {
	log := logger.WithHostnameAndIP(req.HostName, req.Ip)

	if !s.objectStorageConfig.Enable {
		msg := "object storage is disabled"
		log.Debug(msg)
		return nil, status.Error(codes.NotFound, msg)
	}

	var pbListBucketsResponse managerv1.ListBucketsResponse
	cacheKey := cache.MakeBucketsCacheKey(s.objectStorageConfig.Name)

	// Cache hit.
	if err := s.cache.Get(ctx, cacheKey, &pbListBucketsResponse); err == nil {
		log.Infof("%s cache hit", cacheKey)
		return &pbListBucketsResponse, nil
	}

	// Cache miss.
	log.Infof("%s cache miss", cacheKey)
	buckets, err := s.objectStorage.ListBucketMetadatas(ctx)
	if err != nil {
		log.Errorf("list bucket metadatas failed: %s", err.Error())
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
		log.Warnf("storage cache failed: %v", err)
	}

	return &pbListBucketsResponse, nil
}

// List models information.
func (s *Server) ListModels(ctx context.Context, req *managerv1.ListModelsRequest) (*managerv1.ListModelsResponse, error) {
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
func (s *Server) GetModel(ctx context.Context, req *managerv1.GetModelRequest) (*managerv1.Model, error) {
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
func (s *Server) CreateModel(ctx context.Context, req *managerv1.CreateModelRequest) (*managerv1.Model, error) {
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
func (s *Server) UpdateModel(ctx context.Context, req *managerv1.UpdateModelRequest) (*managerv1.Model, error) {
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
func (s *Server) DeleteModel(ctx context.Context, req *managerv1.DeleteModelRequest) (*emptypb.Empty, error) {
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
func (s *Server) ListModelVersions(ctx context.Context, req *managerv1.ListModelVersionsRequest) (*managerv1.ListModelVersionsResponse, error) {
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
func (s *Server) GetModelVersion(ctx context.Context, req *managerv1.GetModelVersionRequest) (*managerv1.ModelVersion, error) {
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
func (s *Server) CreateModelVersion(ctx context.Context, req *managerv1.CreateModelVersionRequest) (*managerv1.ModelVersion, error) {
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
func (s *Server) UpdateModelVersion(ctx context.Context, req *managerv1.UpdateModelVersionRequest) (*managerv1.ModelVersion, error) {
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
func (s *Server) DeleteModelVersion(ctx context.Context, req *managerv1.DeleteModelVersionRequest) (*emptypb.Empty, error) {
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

// KeepAlive with manager.
func (s *Server) KeepAlive(stream managerv1.Manager_KeepAliveServer) error {
	req, err := stream.Recv()
	if err != nil {
		logger.Errorf("keepalive failed for the first time: %v", err)
		return status.Error(codes.Unknown, err.Error())
	}
	hostName := req.HostName
	ip := req.Ip
	sourceType := req.SourceType
	clusterID := uint(req.ClusterId)
	logger.Infof("%s keepalive successfully for the first time in cluster %d", hostName, clusterID)

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
			logger.Warnf("%s refresh keepalive status failed in scheduler cluster %d", hostName, clusterID)
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
			logger.Warnf("%s refresh keepalive status failed in seed peer cluster %d", hostName, clusterID)
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
					logger.Warnf("%s refresh keepalive status failed in scheduler cluster %d", hostName, clusterID)
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
					logger.Warnf("%s refresh keepalive status failed in seed peer cluster %d", hostName, clusterID)
				}
			}

			if err == io.EOF {
				logger.Infof("%s close keepalive in cluster %d", hostName, clusterID)
				return nil
			}

			logger.Errorf("%s keepalive failed in cluster %d: %v", hostName, clusterID, err)
			return status.Error(codes.Unknown, err.Error())
		}

		logger.Debugf("%s type of %s send keepalive request in cluster %d", sourceType, hostName, clusterID)
	}
}

// Get scheduler cluster names.
func getSchedulerClusterNames(clusters []model.SchedulerCluster) []string {
	names := []string{}
	for _, cluster := range clusters {
		names = append(names, cluster.Name)
	}

	return names
}
