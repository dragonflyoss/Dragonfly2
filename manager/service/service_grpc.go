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

	// Cache Hit
	if err := s.cache.Get(ctx, cacheKey, &pbCDN); err == nil {
		logger.Infof("%s cache hit", cacheKey)
		return &pbCDN, nil
	}

	// Cache Miss
	logger.Infof("%s cache miss", cacheKey)
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
		logger.Warnf("Storage cache failed: %v", err)
	}

	return &pbCDN, nil
}

func (s *ServiceGRPC) GetScheduler(ctx context.Context, req *manager.GetSchedulerRequest) (*manager.Scheduler, error) {
	if err := req.Validate(); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	var pbScheduler manager.Scheduler
	cacheKey := cache.MakeCacheKey("scheduler", req.HostName)

	// Cache Hit
	if err := s.cache.Get(ctx, cacheKey, &pbScheduler); err == nil {
		logger.Infof("%s cache hit", cacheKey)
		return &pbScheduler, nil
	}

	// Cache Miss
	logger.Infof("%s cache miss", cacheKey)
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
		logger.Warnf("Cache storage failed: %v", err)
	}

	return &pbScheduler, nil
}

func (s *ServiceGRPC) ListSchedulers(ctx context.Context, req *manager.ListSchedulersRequest) (*manager.ListSchedulersResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	var pbListSchedulersResponse manager.ListSchedulersResponse
	cacheKey := cache.MakeCacheKey("schedulers", req.HostName)

	// Cache Hit
	if err := s.cache.Get(ctx, cacheKey, &pbListSchedulersResponse); err == nil {
		logger.Infof("%s cache hit", cacheKey)
		return &pbListSchedulersResponse, nil
	}

	// Cache Miss
	logger.Infof("%s cache miss", cacheKey)
	schedulers := []model.Scheduler{}
	if err := s.db.Find(&schedulers).Error; err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
	}

	for _, scheduler := range schedulers {
		schedulerNetConfig, err := scheduler.NetConfig.MarshalJSON()
		if err != nil {
			return nil, status.Error(codes.DataLoss, err.Error())
		}

		pbListSchedulersResponse.Schedulers = append(pbListSchedulersResponse.Schedulers, &manager.Scheduler{
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
		Value: &pbListSchedulersResponse,
		TTL:   s.cache.TTL,
	}); err != nil {
		logger.Warnf("Storage cache failed: %v", err)
	}

	return &pbListSchedulersResponse, nil
}

func (s *ServiceGRPC) KeepAlive(ctx context.Context, req *manager.KeepAliveRequest) (*empty.Empty, error) {
	if err := req.Validate(); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return nil, nil
}
