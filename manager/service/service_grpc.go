package service

import (
	"context"

	"d7y.io/dragonfly/v2/internal/rpc/manager"
	"d7y.io/dragonfly/v2/manager/cache"
	"d7y.io/dragonfly/v2/manager/database"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ServiceGRPC interface {
	GetCDN(context.Context, *manager.GetCDNRequest) (*manager.CDN, error)
	GetScheduler(context.Context, *manager.GetSchedulerRequest) (*manager.Scheduler, error)
	ListSchedulers(context.Context, *manager.ListSchedulersRequest) (*manager.ListSchedulersResponse, error)
	KeepAlive(context.Context, *manager.KeepAliveRequest) (*empty.Empty, error)
}

type serviceGRPC struct {
	db    *database.Database
	cache *cache.Cache
}

// Option is a functional option for service
type GRPCOption func(s *serviceGRPC)

// WithDatabase set the database client
func GRPCWithDatabase(database *database.Database) GRPCOption {
	return func(s *serviceGRPC) {
		s.db = database
	}
}

// WithCache set the cache client
func GRPCWithCache(cache *cache.Cache) GRPCOption {
	return func(s *serviceGRPC) {
		s.cache = cache
	}
}

// New returns a new Service instence
func NewGRPC(options ...GRPCOption) ServiceGRPC {
	s := &serviceGRPC{}

	for _, opt := range options {
		opt(s)
	}

	return s
}

func (s *serviceGRPC) GetCDN(ctx context.Context, req *manager.GetCDNRequest) (*manager.CDN, error) {
	if err := req.Validate(); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return nil, nil
}

func (s *serviceGRPC) GetScheduler(ctx context.Context, req *manager.GetSchedulerRequest) (*manager.Scheduler, error) {
	if err := req.Validate(); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return nil, nil
}

func (s *serviceGRPC) ListSchedulers(ctx context.Context, req *manager.ListSchedulersRequest) (*manager.ListSchedulersResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return nil, nil
}

func (s *serviceGRPC) KeepAlive(ctx context.Context, req *manager.KeepAliveRequest) (*empty.Empty, error) {
	if err := req.Validate(); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return nil, nil
}
