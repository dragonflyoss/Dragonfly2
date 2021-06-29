package service

import (
	"context"
	"io"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/rpc/manager"
	"d7y.io/dragonfly/v2/manager/cache"
	"d7y.io/dragonfly/v2/manager/database"
	"d7y.io/dragonfly/v2/manager/model"
	cachev8 "github.com/go-redis/cache/v8"
	"github.com/go-redis/redis/v8"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type ServiceGRPC struct {
	db    *gorm.DB
	rdb   *redis.Client
	cache *cache.Cache
	manager.UnimplementedManagerServer
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
	if err := s.db.Preload("CDNCluster.SecurityGroup").First(&cdn, &model.CDN{
		HostName: req.HostName,
	}).Error; err != nil {
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
		logger.Warnf("storage cache failed: %v", err)
	}

	return &pbCDN, nil
}

func (s *ServiceGRPC) CreateCDN(ctx context.Context, req *manager.CreateCDNRequest) (*manager.CDN, error) {
	if err := req.Validate(); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	cdn := model.CDN{
		HostName:     req.HostName,
		IDC:          req.Idc,
		Location:     req.Location,
		IP:           req.Ip,
		Port:         req.Port,
		DownloadPort: req.DownloadPort,
	}

	if err := s.db.Create(&cdn).Error; err != nil {
		return nil, err
	}

	return &manager.CDN{
		Id:           uint64(cdn.ID),
		HostName:     cdn.HostName,
		Location:     cdn.Location,
		Ip:           cdn.IP,
		Port:         cdn.Port,
		DownloadPort: cdn.DownloadPort,
		Status:       cdn.Status,
	}, nil
}

func (s *ServiceGRPC) UpdateCDN(ctx context.Context, req *manager.UpdateCDNRequest) (*manager.CDN, error) {
	if err := req.Validate(); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	cdn := model.CDN{}
	if err := s.db.First(&cdn, model.CDN{HostName: req.HostName}).Updates(model.CDN{
		IDC:          req.Idc,
		Location:     req.Location,
		IP:           req.Ip,
		Port:         req.Port,
		DownloadPort: req.DownloadPort,
	}).Error; err != nil {
		return nil, err
	}

	return &manager.CDN{
		Id:           uint64(cdn.ID),
		HostName:     cdn.HostName,
		Location:     cdn.Location,
		Ip:           cdn.IP,
		Port:         cdn.Port,
		DownloadPort: cdn.DownloadPort,
		Status:       cdn.Status,
	}, nil
}

func (s *ServiceGRPC) AddCDNToCDNCluster(ctx context.Context, req *manager.AddCDNToCDNClusterRequest) (*emptypb.Empty, error) {
	if err := req.Validate(); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	cdnCluster := model.CDNCluster{}
	if err := s.db.First(&cdnCluster, req.CdnClusterId).Error; err != nil {
		return nil, err
	}

	cdn := model.CDN{}
	if err := s.db.First(&cdn, req.CdnId).Error; err != nil {
		return nil, err
	}

	if err := s.db.Model(&cdnCluster).Association("CDNs").Append(&cdn); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
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
	if err := s.db.Preload("SchedulerCluster.SecurityGroup").Preload("SchedulerCluster.CDNClusters.CDNs", &model.CDN{
		Status: model.CDNStatusActive,
	}).First(&scheduler, &model.Scheduler{
		HostName: req.HostName,
	}).Error; err != nil {
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
		logger.Warnf("cache storage failed: %v", err)
	}

	return &pbScheduler, nil
}

func (s *ServiceGRPC) CreateScheduler(ctx context.Context, req *manager.CreateSchedulerRequest) (*manager.Scheduler, error) {
	if err := req.Validate(); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	var netConfig datatypes.JSONMap
	if len(req.NetConfig) > 0 {
		if err := netConfig.UnmarshalJSON(req.NetConfig); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
	}

	scheduler := model.Scheduler{
		HostName:  req.HostName,
		VIPs:      req.Vips,
		IDC:       req.Idc,
		Location:  req.Location,
		NetConfig: netConfig,
		IP:        req.Ip,
		Port:      req.Port,
	}

	if err := s.db.Create(&scheduler).Error; err != nil {
		return nil, err
	}

	return &manager.Scheduler{
		Id:        uint64(scheduler.ID),
		HostName:  scheduler.HostName,
		Vips:      scheduler.VIPs,
		Idc:       scheduler.IDC,
		Location:  scheduler.Location,
		NetConfig: req.NetConfig,
		Ip:        scheduler.IP,
		Port:      scheduler.Port,
		Status:    scheduler.Status,
	}, nil
}

func (s *ServiceGRPC) UpdateScheduler(ctx context.Context, req *manager.UpdateSchedulerRequest) (*manager.Scheduler, error) {
	if err := req.Validate(); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	var netConfig datatypes.JSONMap
	if len(req.NetConfig) > 0 {
		if err := netConfig.UnmarshalJSON(req.NetConfig); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
	}

	scheduler := model.Scheduler{}
	if err := s.db.First(&scheduler, model.Scheduler{HostName: req.HostName}).Updates(model.Scheduler{
		VIPs:      req.Vips,
		IDC:       req.Idc,
		Location:  req.Location,
		NetConfig: netConfig,
		IP:        req.Ip,
		Port:      req.Port,
	}).Error; err != nil {
		return nil, err
	}

	return &manager.Scheduler{
		Id:        uint64(scheduler.ID),
		HostName:  scheduler.HostName,
		Vips:      scheduler.VIPs,
		Idc:       scheduler.IDC,
		Location:  scheduler.Location,
		NetConfig: req.NetConfig,
		Ip:        scheduler.IP,
		Port:      scheduler.Port,
		Status:    scheduler.Status,
	}, nil
}

func (s *ServiceGRPC) AddSchedulerClusterToSchedulerCluster(ctx context.Context, req *manager.AddSchedulerClusterToSchedulerClusterRequest) (*emptypb.Empty, error) {
	if err := req.Validate(); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	schedulerCluster := model.SchedulerCluster{}
	if err := s.db.First(&schedulerCluster, req.SchedulerClusterId).Error; err != nil {
		return nil, err
	}

	scheduler := model.Scheduler{}
	if err := s.db.First(&scheduler, req.SchedulerId).Error; err != nil {
		return nil, err
	}

	if err := s.db.Model(&schedulerCluster).Association("Schedulers").Append(&scheduler); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
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
	if err := s.db.Find(&schedulers, &model.Scheduler{
		Status: model.SchedulerStatusActive,
	}).Error; err != nil {
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
		logger.Warnf("storage cache failed: %v", err)
	}

	return &pbListSchedulersResponse, nil
}

func (s *ServiceGRPC) KeepAlive(m manager.Manager_KeepAliveServer) error {
	req, err := m.Recv()
	if err != nil {
		logger.Errorf("keepalive failed for the first time: %v", err)
		return err
	}
	if err := req.Validate(); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	var cachekey string
	hostName := req.HostName
	sourceType := req.SourceType
	logger.Infof("%s keepalive successfully for the first time", req.HostName)

	// Active scheduler
	if sourceType == manager.SourceType_SCHEDULER_SOURCE {
		scheduler := model.Scheduler{}
		if err := s.db.First(&scheduler, model.Scheduler{
			HostName: hostName,
		}).Updates(model.Scheduler{
			Status: model.SchedulerStatusActive,
		}).Error; err != nil {
			return err
		}

		cachekey = cache.MakeCacheKey("scheduler", hostName)
		if err := s.cache.Delete(context.TODO(), cachekey); err != nil {
			logger.Warnf("%s refresh keepalive status failed", req.HostName)
		}
	}

	// Active CDN
	if sourceType == manager.SourceType_CDN_SOURCE {
		cdn := model.CDN{}
		if err := s.db.First(&cdn, model.CDN{
			HostName: hostName,
		}).Updates(model.CDN{
			Status: model.CDNStatusActive,
		}).Error; err != nil {
			return err
		}

		cachekey = cache.MakeCacheKey("cdn", hostName)
		if err := s.cache.Delete(context.TODO(), cachekey); err != nil {
			logger.Warnf("%s refresh keepalive status failed", req.HostName)
		}
	}

	for {
		_, err := m.Recv()
		if err != nil {
			// Inactive scheduler
			if sourceType == manager.SourceType_SCHEDULER_SOURCE {
				scheduler := model.Scheduler{}
				if err := s.db.First(&scheduler, model.Scheduler{
					HostName: hostName,
				}).Updates(model.Scheduler{
					Status: model.SchedulerStatusInactive,
				}).Error; err != nil {
					return err
				}

				cachekey = cache.MakeCacheKey("scheduler", hostName)
				if err := s.cache.Delete(context.TODO(), cachekey); err != nil {
					logger.Warnf("%s refresh keepalive status failed", req.HostName)
				}
			}

			// Inactive CDN
			if sourceType == manager.SourceType_CDN_SOURCE {
				cdn := model.CDN{}
				if err := s.db.First(&cdn, model.CDN{
					HostName: hostName,
				}).Updates(model.CDN{
					Status: model.CDNStatusInactive,
				}).Error; err != nil {
					return err
				}

				cachekey = cache.MakeCacheKey("cdn", hostName)
				if err := s.cache.Delete(context.TODO(), cachekey); err != nil {
					logger.Warnf("%s refresh keepalive status failed", req.HostName)
				}
			}

			if err == io.EOF {
				logger.Infof("%s close keepalive", hostName)
				return nil
			}
			logger.Errorf("%s keepalive failed: %v", hostName, err)
			return err
		}

		logger.Debugf("%s send keepalive request", hostName)
	}
}
