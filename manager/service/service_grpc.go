package service

import (
	"context"

	"d7y.io/dragonfly/v2/internal/rpc/manager"
	"d7y.io/dragonfly/v2/manager/cache"
	"d7y.io/dragonfly/v2/manager/database"
	"d7y.io/dragonfly/v2/manager/model"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ServiceGRPC struct {
	db    *database.Database
	cache *cache.Cache
}

// Option is a functional option for service
type GRPCOption func(s *ServiceGRPC)

// WithDatabase set the database client
func GRPCWithDatabase(database *database.Database) GRPCOption {
	return func(s *ServiceGRPC) {
		s.db = database
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

	cdn := model.CDN{}
	if err := s.db.First(&cdn, &model.CDN{HostName: req.HostName}).Error; err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
	}

	cdnCreatedAt, err := ptypes.TimestampProto(cdn.CreatedAt)
	if err != nil {
		return nil, status.Error(codes.DataLoss, err.Error())
	}

	cdnUpdatedAt, err := ptypes.TimestampProto(cdn.UpdatedAt)
	if err != nil {
		return nil, status.Error(codes.DataLoss, err.Error())
	}

	cdnCluster := model.CDNCluster{}
	if err := s.db.First(&cdnCluster, cdn.CDNClusterID).Error; err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
	}

	config, err := cdn.CDNCluster.Config.MarshalJSON()
	if err != nil {
		return nil, status.Error(codes.DataLoss, err.Error())
	}

	clusterCreatedAt, err := ptypes.TimestampProto(cdnCluster.CreatedAt)
	if err != nil {
		return nil, status.Error(codes.DataLoss, err.Error())
	}

	clusterUpdatedAt, err := ptypes.TimestampProto(cdnCluster.UpdatedAt)
	if err != nil {
		return nil, status.Error(codes.DataLoss, err.Error())
	}

	return &manager.CDN{
		Id:           uint64(cdn.ID),
		HostName:     cdn.HostName,
		Idc:          cdn.IDC,
		Location:     cdn.Location,
		Ip:           cdn.IP,
		Port:         cdn.Port,
		DownloadPort: cdn.DownloadPort,
		Status:       cdn.Status,
		CdnClusterId: uint64(*cdn.CDNClusterID),
		CdnCluster: &manager.CDNCluster{
			Id:        uint64(cdnCluster.ID),
			Name:      cdnCluster.Name,
			Bio:       cdnCluster.BIO,
			Config:    config,
			CreatedAt: clusterCreatedAt,
			UpdatedAt: clusterUpdatedAt,
		},
		CreatedAt: cdnCreatedAt,
		UpdatedAt: cdnUpdatedAt,
	}, nil
}

func (s *ServiceGRPC) GetScheduler(ctx context.Context, req *manager.GetSchedulerRequest) (*manager.Scheduler, error) {
	if err := req.Validate(); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return nil, nil
}

func (s *ServiceGRPC) ListSchedulers(ctx context.Context, req *manager.ListSchedulersRequest) (*manager.ListSchedulersResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return nil, nil
}

func (s *ServiceGRPC) KeepAlive(ctx context.Context, req *manager.KeepAliveRequest) (*empty.Empty, error) {
	if err := req.Validate(); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return nil, nil
}
