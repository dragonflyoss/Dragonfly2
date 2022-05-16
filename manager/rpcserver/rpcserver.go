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
	"context"
	"errors"
	"io"

	cachev8 "github.com/go-redis/cache/v8"
	"github.com/go-redis/redis/v8"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	grpc_validator "github.com/grpc-ecosystem/go-grpc-middleware/validator"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/manager/cache"
	"d7y.io/dragonfly/v2/manager/database"
	"d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/manager/searcher"
	"d7y.io/dragonfly/v2/pkg/rpc/manager"
)

// Default middlewares for stream.
func defaultStreamMiddleWares() []grpc.StreamServerInterceptor {
	return []grpc.StreamServerInterceptor{
		grpc_validator.StreamServerInterceptor(),
		grpc_recovery.StreamServerInterceptor(),
		grpc_prometheus.StreamServerInterceptor,
		grpc_zap.StreamServerInterceptor(logger.GrpcLogger.Desugar()),
	}
}

// Default middlewares for unary.
func defaultUnaryMiddleWares() []grpc.UnaryServerInterceptor {
	return []grpc.UnaryServerInterceptor{
		grpc_validator.UnaryServerInterceptor(),
		grpc_recovery.UnaryServerInterceptor(),
		grpc_prometheus.UnaryServerInterceptor,
		grpc_zap.UnaryServerInterceptor(logger.GrpcLogger.Desugar()),
	}
}

// Server is grpc server.
type Server struct {
	// GORM instance.
	db *gorm.DB
	// Redis client instance.
	rdb *redis.Client
	// Cache instance.
	cache *cache.Cache
	// Searcher interface.
	searcher searcher.Searcher
	// Manager grpc interface.
	manager.UnimplementedManagerServer
}

// New returns a new manager server from the given options.
func New(database *database.Database, cache *cache.Cache, searcher searcher.Searcher, opts ...grpc.ServerOption) *grpc.Server {
	server := &Server{
		db:       database.DB,
		rdb:      database.RDB,
		cache:    cache,
		searcher: searcher,
	}

	grpcServer := grpc.NewServer(append([]grpc.ServerOption{
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(defaultStreamMiddleWares()...)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(defaultUnaryMiddleWares()...)),
	}, opts...)...)

	// Register servers on grpc server.
	manager.RegisterManagerServer(grpcServer, server)
	healthpb.RegisterHealthServer(grpcServer, health.NewServer())
	return grpcServer
}

// Get SeedPeer and SeedPeer cluster configuration.
func (s *Server) GetSeedPeer(ctx context.Context, req *manager.GetSeedPeerRequest) (*manager.SeedPeer, error) {
	var pbSeedPeer manager.SeedPeer
	cacheKey := cache.MakeSeedPeerCacheKey(req.HostName, uint(req.SeedPeerClusterId))

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
	var pbSchedulers []*manager.Scheduler
	for _, schedulerCluster := range seedPeer.SeedPeerCluster.SchedulerClusters {
		for _, scheduler := range schedulerCluster.Schedulers {
			pbSchedulers = append(pbSchedulers, &manager.Scheduler{
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
	pbSeedPeer = manager.SeedPeer{
		Id:                uint64(seedPeer.ID),
		Type:              seedPeer.Type,
		IsCdn:             seedPeer.IsCDN,
		HostName:          seedPeer.HostName,
		Idc:               seedPeer.IDC,
		NetTopology:       seedPeer.NetTopology,
		Location:          seedPeer.Location,
		Ip:                seedPeer.IP,
		Port:              seedPeer.Port,
		DownloadPort:      seedPeer.DownloadPort,
		State:             seedPeer.State,
		SeedPeerClusterId: uint64(seedPeer.SeedPeerClusterID),
		SeedPeerCluster: &manager.SeedPeerCluster{
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
func (s *Server) UpdateSeedPeer(ctx context.Context, req *manager.UpdateSeedPeerRequest) (*manager.SeedPeer, error) {
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
		IsCDN:             req.IsCdn,
		IDC:               req.Idc,
		NetTopology:       req.NetTopology,
		Location:          req.Location,
		IP:                req.Ip,
		Port:              req.Port,
		DownloadPort:      req.DownloadPort,
		SeedPeerClusterID: uint(req.SeedPeerClusterId),
	}).Error; err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
	}

	if err := s.cache.Delete(
		ctx,
		cache.MakeSeedPeerCacheKey(seedPeer.HostName, seedPeer.SeedPeerClusterID),
	); err != nil {
		logger.Warnf("%s refresh keepalive status failed in seed peer cluster %d", seedPeer.HostName, seedPeer.SeedPeerClusterID)
	}

	return &manager.SeedPeer{
		Id:                uint64(seedPeer.ID),
		HostName:          seedPeer.HostName,
		Type:              seedPeer.Type,
		IsCdn:             seedPeer.IsCDN,
		Idc:               seedPeer.IDC,
		NetTopology:       seedPeer.NetTopology,
		Location:          seedPeer.Location,
		Ip:                seedPeer.IP,
		Port:              seedPeer.Port,
		DownloadPort:      seedPeer.DownloadPort,
		State:             seedPeer.State,
		SeedPeerClusterId: uint64(seedPeer.SeedPeerClusterID),
	}, nil
}

// Create SeedPeer and associate cluster.
func (s *Server) createSeedPeer(ctx context.Context, req *manager.UpdateSeedPeerRequest) (*manager.SeedPeer, error) {
	seedPeer := model.SeedPeer{
		HostName:          req.HostName,
		Type:              req.Type,
		IsCDN:             req.IsCdn,
		IDC:               req.Idc,
		NetTopology:       req.NetTopology,
		Location:          req.Location,
		IP:                req.Ip,
		Port:              req.Port,
		DownloadPort:      req.DownloadPort,
		SeedPeerClusterID: uint(req.SeedPeerClusterId),
	}

	if err := s.db.WithContext(ctx).Create(&seedPeer).Error; err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
	}

	return &manager.SeedPeer{
		Id:                uint64(seedPeer.ID),
		HostName:          seedPeer.HostName,
		Type:              seedPeer.Type,
		IsCdn:             seedPeer.IsCDN,
		Idc:               seedPeer.IDC,
		NetTopology:       seedPeer.NetTopology,
		Location:          seedPeer.Location,
		Ip:                seedPeer.IP,
		Port:              seedPeer.Port,
		DownloadPort:      seedPeer.DownloadPort,
		SeedPeerClusterId: uint64(seedPeer.SeedPeerClusterID),
		State:             seedPeer.State,
	}, nil
}

// Get Scheduler and Scheduler cluster configuration.
func (s *Server) GetScheduler(ctx context.Context, req *manager.GetSchedulerRequest) (*manager.Scheduler, error) {
	var pbScheduler manager.Scheduler
	cacheKey := cache.MakeSchedulerCacheKey(req.HostName, uint(req.SchedulerClusterId))

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
	var pbSeedPeers []*manager.SeedPeer
	for _, seedPeerCluster := range scheduler.SchedulerCluster.SeedPeerClusters {
		seedPeerClusterConfig, err := seedPeerCluster.Config.MarshalJSON()
		if err != nil {
			return nil, status.Error(codes.DataLoss, err.Error())
		}

		for _, seedPeer := range seedPeerCluster.SeedPeers {
			pbSeedPeers = append(pbSeedPeers, &manager.SeedPeer{
				Id:                uint64(seedPeer.ID),
				HostName:          seedPeer.HostName,
				Type:              seedPeer.Type,
				IsCdn:             seedPeer.IsCDN,
				Idc:               seedPeer.IDC,
				NetTopology:       seedPeer.NetTopology,
				Location:          seedPeer.Location,
				Ip:                seedPeer.IP,
				Port:              seedPeer.Port,
				DownloadPort:      seedPeer.DownloadPort,
				State:             seedPeer.State,
				SeedPeerClusterId: uint64(seedPeer.SeedPeerClusterID),
				SeedPeerCluster: &manager.SeedPeerCluster{
					Id:     uint64(seedPeerCluster.ID),
					Name:   seedPeerCluster.Name,
					Bio:    seedPeerCluster.BIO,
					Config: seedPeerClusterConfig,
				},
			})
		}
	}

	// Construct scheduler.
	pbScheduler = manager.Scheduler{
		Id:                 uint64(scheduler.ID),
		HostName:           scheduler.HostName,
		Idc:                scheduler.IDC,
		NetTopology:        scheduler.NetTopology,
		Location:           scheduler.Location,
		Ip:                 scheduler.IP,
		Port:               scheduler.Port,
		State:              scheduler.State,
		SchedulerClusterId: uint64(scheduler.SchedulerClusterID),
		SchedulerCluster: &manager.SchedulerCluster{
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
func (s *Server) UpdateScheduler(ctx context.Context, req *manager.UpdateSchedulerRequest) (*manager.Scheduler, error) {
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
		cache.MakeSchedulerCacheKey(scheduler.HostName, scheduler.SchedulerClusterID),
	); err != nil {
		logger.Warnf("%s refresh keepalive status failed in scheduler cluster %d", scheduler.HostName, scheduler.SchedulerClusterID)
	}

	return &manager.Scheduler{
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
func (s *Server) createScheduler(ctx context.Context, req *manager.UpdateSchedulerRequest) (*manager.Scheduler, error) {
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

	return &manager.Scheduler{
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
func (s *Server) ListSchedulers(ctx context.Context, req *manager.ListSchedulersRequest) (*manager.ListSchedulersResponse, error) {
	log := logger.WithHostnameAndIP(req.HostName, req.Ip)

	var pbListSchedulersResponse manager.ListSchedulersResponse
	cacheKey := cache.MakeSchedulersCacheKey(req.HostName, req.Ip)

	// Cache hit.
	if err := s.cache.Get(ctx, cacheKey, &pbListSchedulersResponse); err == nil {
		log.Infof("%s cache hit", cacheKey)
		return &pbListSchedulersResponse, nil
	}

	// Cache miss.
	log.Infof("%s cache miss", cacheKey)
	var schedulerClusters []model.SchedulerCluster
	if err := s.db.WithContext(ctx).Preload("SecurityGroup.SecurityRules").Preload("Schedulers", "state = ?", "active").Find(&schedulerClusters).Error; err != nil {
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
			schedulers = append(schedulers, scheduler)
		}
	}

	// Construct schedulers.
	for _, scheduler := range schedulers {
		pbListSchedulersResponse.Schedulers = append(pbListSchedulersResponse.Schedulers, &manager.Scheduler{
			Id:                 uint64(scheduler.ID),
			HostName:           scheduler.HostName,
			Idc:                scheduler.IDC,
			NetTopology:        scheduler.NetTopology,
			Location:           scheduler.Location,
			Ip:                 scheduler.IP,
			Port:               scheduler.Port,
			State:              scheduler.State,
			SchedulerClusterId: uint64(scheduler.SchedulerClusterID),
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

// KeepAlive with manager.
func (s *Server) KeepAlive(stream manager.Manager_KeepAliveServer) error {
	req, err := stream.Recv()
	if err != nil {
		logger.Errorf("keepalive failed for the first time: %v", err)
		return status.Error(codes.Unknown, err.Error())
	}
	hostName := req.HostName
	sourceType := req.SourceType
	clusterID := uint(req.ClusterId)
	logger.Infof("%s keepalive successfully for the first time in cluster %d", hostName, clusterID)

	// Active scheduler.
	if sourceType == manager.SourceType_SCHEDULER_SOURCE {
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
			cache.MakeSchedulerCacheKey(hostName, clusterID),
		); err != nil {
			logger.Warnf("%s refresh keepalive status failed in scheduler cluster %d", hostName, clusterID)
		}
	}

	// Active seed peer.
	if sourceType == manager.SourceType_SEED_PEER_SOURCE {
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
			cache.MakeSeedPeerCacheKey(hostName, clusterID),
		); err != nil {
			logger.Warnf("%s refresh keepalive status failed in seed peer cluster %d", hostName, clusterID)
		}
	}

	for {
		_, err := stream.Recv()
		if err != nil {
			// Inactive scheduler.
			if sourceType == manager.SourceType_SCHEDULER_SOURCE {
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
					cache.MakeSchedulerCacheKey(hostName, clusterID),
				); err != nil {
					logger.Warnf("%s refresh keepalive status failed in scheduler cluster %d", hostName, clusterID)
				}
			}

			// Inactive seed peer.
			if sourceType == manager.SourceType_SEED_PEER_SOURCE {
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
					cache.MakeSeedPeerCacheKey(hostName, clusterID),
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
