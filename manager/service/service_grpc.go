package service

import (
	"context"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/rpc/manager"
	"d7y.io/dragonfly/v2/manager/cache"
	"d7y.io/dragonfly/v2/manager/database"
	"d7y.io/dragonfly/v2/manager/model"
	cachev8 "github.com/go-redis/cache/v8"
	"github.com/go-redis/redis/v8"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
)

type ServiceGRPC struct {
	db    *gorm.DB
	rdb   *redis.Client
	cache *cache.Cache
}

// Option is a functional option for service
type GRPCOption func(s *ServiceGRPC)

// WithDatabase set the database client
func GRPCWithDatabase(database *database.Database) GRPCOption {
	return func(s *ServiceGRPC) {
		s.db = database.DB
		s.rdb = database.RDB
	}
}

// WithCache set the cache client
func GRPCWithCache(cache *cache.Cache) GRPCOption {
	return func(s *ServiceGRPC) {
		s.cache = cache
	}
}

// New returns a new Service instence
func NewGRPC(options ...GRPCOption) *ServiceGRPC {
	s := &ServiceGRPC{}

	for _, opt := range options {
		opt(s)
	}

	return s
}

func (s *ServiceGRPC) GetCDN(ctx context.Context, req *manager.GetCDNRequest) (*manager.CDN, error) {
	if err := req.Validate(); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	var pbCDN manager.CDN
	cacheKey := cache.MakeCacheKey("cdn", req.HostName)
	if err := s.cache.Get(ctx, cacheKey, &pbCDN); err != nil {
		logger.Infof("Cache Invalidation: %s", err.Error())

		cdn := model.CDN{}
		if err := s.db.Preload("CDNCluster.SecurityGroup").First(&cdn, &model.CDN{HostName: req.HostName}).Error; err != nil {
			return nil, status.Error(codes.Unknown, err.Error())
		}

		config, err := cdn.CDNCluster.Config.MarshalJSON()
		if err != nil {
			return nil, status.Error(codes.DataLoss, err.Error())
		}

		pbCDN = manager.CDN{
			Id:           uint64(cdn.ID),
			HostName:     cdn.HostName,
			Idc:          cdn.IDC,
			Location:     cdn.Location,
			Ip:           cdn.IP,
			Port:         cdn.Port,
			DownloadPort: cdn.DownloadPort,
			Status:       cdn.Status,
			CdnCluster: &manager.CDNCluster{
				Id:     uint64(cdn.CDNCluster.ID),
				Name:   cdn.CDNCluster.Name,
				Bio:    cdn.CDNCluster.BIO,
				Config: config,
				SecurityGroup: &manager.SecurityGroup{
					Id:          uint64(cdn.CDNCluster.SecurityGroup.ID),
					Name:        cdn.CDNCluster.SecurityGroup.Name,
					Bio:         cdn.CDNCluster.SecurityGroup.BIO,
					Domain:      cdn.CDNCluster.SecurityGroup.Domain,
					ProxyDomain: cdn.CDNCluster.SecurityGroup.ProxyDomain,
				},
			},
		}

		if err := s.cache.Once(&cachev8.Item{
			Ctx:   ctx,
			Key:   cacheKey,
			Value: &pbCDN,
			TTL:   s.cache.TTL,
		}); err != nil {
			logger.Warnf("Storage cache failed: %s", err.Error())
		}
	}

	return &pbCDN, nil
}

func (s *ServiceGRPC) GetScheduler(ctx context.Context, req *manager.GetSchedulerRequest) (*manager.Scheduler, error) {
	if err := req.Validate(); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	var pbScheduler manager.Scheduler
	cacheKey := cache.MakeCacheKey("scheduler", req.HostName)
	if err := s.cache.Get(ctx, cacheKey, &pbScheduler); err != nil {
		logger.Infof("Cache Invalidation: %s", err.Error())

		scheduler := model.Scheduler{}
		if err := s.db.Preload("SchedulerCluster.SecurityGroup").Preload("SchedulerCluster.CDNClusters.CDNs").First(&scheduler, &model.Scheduler{HostName: req.HostName}).Error; err != nil {
			return nil, status.Error(codes.Unknown, err.Error())
		}

		schedulerNetConfig, err := scheduler.NetConfig.MarshalJSON()
		if err != nil {
			return nil, status.Error(codes.DataLoss, err.Error())
		}

		schedulerClusterConfig, err := scheduler.SchedulerCluster.Config.MarshalJSON()
		if err != nil {
			return nil, status.Error(codes.DataLoss, err.Error())
		}

		schedulerClusterClientConfig, err := scheduler.SchedulerCluster.ClientConfig.MarshalJSON()
		if err != nil {
			return nil, status.Error(codes.DataLoss, err.Error())
		}

		securityGroup := model.SecurityGroup{}
		if err := s.db.First(&securityGroup, scheduler.SchedulerCluster.SecurityGroupID).Error; err != nil {
			return nil, status.Error(codes.Unknown, err.Error())
		}

		var pbCDNs []*manager.CDN
		for _, cdnCluster := range scheduler.SchedulerCluster.CDNClusters {
			for _, cdn := range cdnCluster.CDNs {
				pbCDNs = append(pbCDNs, &manager.CDN{
					Id:           uint64(cdn.ID),
					HostName:     cdn.HostName,
					Idc:          cdn.IDC,
					Location:     cdn.Location,
					Ip:           cdn.IP,
					Port:         cdn.Port,
					DownloadPort: cdn.DownloadPort,
					Status:       cdn.Status,
				})
			}
		}

		pbScheduler = manager.Scheduler{
			Id:        uint64(scheduler.ID),
			HostName:  scheduler.HostName,
			Vips:      scheduler.VIPs,
			Idc:       scheduler.IDC,
			Location:  scheduler.Location,
			NetConfig: schedulerNetConfig,
			Ip:        scheduler.IP,
			Port:      scheduler.Port,
			Status:    scheduler.Status,
			SchedulerCluster: &manager.SchedulerCluster{
				Id:           uint64(scheduler.SchedulerCluster.ID),
				Name:         scheduler.SchedulerCluster.Name,
				Bio:          scheduler.SchedulerCluster.BIO,
				Config:       schedulerClusterConfig,
				ClientConfig: schedulerClusterClientConfig,
				SecurityGroup: &manager.SecurityGroup{
					Id:          uint64(scheduler.SchedulerCluster.SecurityGroup.ID),
					Name:        scheduler.SchedulerCluster.SecurityGroup.Name,
					Bio:         scheduler.SchedulerCluster.SecurityGroup.BIO,
					Domain:      scheduler.SchedulerCluster.SecurityGroup.Domain,
					ProxyDomain: scheduler.SchedulerCluster.SecurityGroup.ProxyDomain,
				},
			},
			Cdns: pbCDNs,
		}

		if err := s.cache.Once(&cachev8.Item{
			Ctx:   ctx,
			Key:   cacheKey,
			Value: &pbScheduler,
			TTL:   s.cache.TTL,
		}); err != nil {
			logger.Warnf("Cache storage failed: %s", err.Error())
		}
	}

	return &pbScheduler, nil
}

func (s *ServiceGRPC) ListSchedulers(ctx context.Context, req *manager.ListSchedulersRequest) (*manager.ListSchedulersResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	var pbSchedulers []*manager.Scheduler
	cacheKey := cache.MakeCacheKey("schedulers", req.HostName)
	if err := s.cache.Get(ctx, cacheKey, &pbSchedulers); err != nil {
		logger.Infof("Cache Invalidation: %s", err.Error())

		schedulers := []model.Scheduler{}
		if err := s.db.Where(&model.Scheduler{Status: model.SchedulerStatusActive}).Find(&schedulers).Error; err != nil {
			return nil, status.Error(codes.Unknown, err.Error())
		}

		for _, scheduler := range schedulers {
			schedulerNetConfig, err := scheduler.NetConfig.MarshalJSON()
			if err != nil {
				return nil, status.Error(codes.DataLoss, err.Error())
			}

			pbSchedulers = append(pbSchedulers, &manager.Scheduler{
				Id:        uint64(scheduler.ID),
				HostName:  scheduler.HostName,
				Vips:      scheduler.VIPs,
				Idc:       scheduler.IDC,
				Location:  scheduler.Location,
				NetConfig: schedulerNetConfig,
				Ip:        scheduler.IP,
				Port:      scheduler.Port,
				Status:    scheduler.Status,
			})
		}

		if err := s.cache.Once(&cachev8.Item{
			Ctx:   ctx,
			Key:   cacheKey,
			Value: pbSchedulers,
			TTL:   s.cache.TTL,
		}); err != nil {
			logger.Warnf("Storage cache failed: %s", err.Error())
		}
	}

	return &manager.ListSchedulersResponse{
		Schedulers: pbSchedulers,
	}, nil
}

func (s *ServiceGRPC) KeepAlive(ctx context.Context, req *manager.KeepAliveRequest) (*empty.Empty, error) {
	if err := req.Validate(); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return nil, nil
}
