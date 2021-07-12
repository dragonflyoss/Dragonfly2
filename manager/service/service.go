package service

import (
	"d7y.io/dragonfly.v2/manager/cache"
	"d7y.io/dragonfly.v2/manager/database"
	"d7y.io/dragonfly.v2/manager/model"
	"d7y.io/dragonfly.v2/manager/types"
	"github.com/go-redis/redis/v8"
	"gorm.io/gorm"
)

type REST interface {
	CreateCDNCluster(types.CreateCDNClusterRequest) (*model.CDNCluster, error)
	CreateCDNClusterWithSecurityGroupDomain(types.CreateCDNClusterRequest) (*model.CDNCluster, error)
	DestroyCDNCluster(uint) error
	UpdateCDNCluster(uint, types.UpdateCDNClusterRequest) (*model.CDNCluster, error)
	UpdateCDNClusterWithSecurityGroupDomain(uint, types.UpdateCDNClusterRequest) (*model.CDNCluster, error)
	GetCDNCluster(uint) (*model.CDNCluster, error)
	GetCDNClusters(types.GetCDNClustersQuery) (*[]model.CDNCluster, error)
	CDNClusterTotalCount(types.GetCDNClustersQuery) (int64, error)
	AddCDNToCDNCluster(uint, uint) error
	AddSchedulerClusterToCDNCluster(uint, uint) error

	CreateCDN(types.CreateCDNRequest) (*model.CDN, error)
	DestroyCDN(uint) error
	UpdateCDN(uint, types.UpdateCDNRequest) (*model.CDN, error)
	GetCDN(uint) (*model.CDN, error)
	GetCDNs(types.GetCDNsQuery) (*[]model.CDN, error)
	CDNTotalCount(types.GetCDNsQuery) (int64, error)

	CreateSchedulerCluster(types.CreateSchedulerClusterRequest) (*model.SchedulerCluster, error)
	CreateSchedulerClusterWithSecurityGroupDomain(types.CreateSchedulerClusterRequest) (*model.SchedulerCluster, error)
	DestroySchedulerCluster(uint) error
	UpdateSchedulerCluster(uint, types.UpdateSchedulerClusterRequest) (*model.SchedulerCluster, error)
	UpdateSchedulerClusterWithSecurityGroupDomain(uint, types.UpdateSchedulerClusterRequest) (*model.SchedulerCluster, error)
	GetSchedulerCluster(uint) (*model.SchedulerCluster, error)
	GetSchedulerClusters(types.GetSchedulerClustersQuery) (*[]model.SchedulerCluster, error)
	SchedulerClusterTotalCount(types.GetSchedulerClustersQuery) (int64, error)
	AddSchedulerToSchedulerCluster(uint, uint) error

	CreateScheduler(types.CreateSchedulerRequest) (*model.Scheduler, error)
	DestroyScheduler(uint) error
	UpdateScheduler(uint, types.UpdateSchedulerRequest) (*model.Scheduler, error)
	GetScheduler(uint) (*model.Scheduler, error)
	GetSchedulers(types.GetSchedulersQuery) (*[]model.Scheduler, error)
	SchedulerTotalCount(types.GetSchedulersQuery) (int64, error)

	CreateSecurityGroup(types.CreateSecurityGroupRequest) (*model.SecurityGroup, error)
	DestroySecurityGroup(uint) error
	UpdateSecurityGroup(uint, types.UpdateSecurityGroupRequest) (*model.SecurityGroup, error)
	GetSecurityGroup(uint) (*model.SecurityGroup, error)
	GetSecurityGroups(types.GetSecurityGroupsQuery) (*[]model.SecurityGroup, error)
	SecurityGroupTotalCount(types.GetSecurityGroupsQuery) (int64, error)
	AddSchedulerClusterToSecurityGroup(uint, uint) error
	AddCDNClusterToSecurityGroup(uint, uint) error

	Login(json types.LoginRequest) (*model.User, error)
	Register(json types.RegisterRequest) (*model.User, error)
}

type rest struct {
	db    *gorm.DB
	rdb   *redis.Client
	cache *cache.Cache
}

// Option is a functional option for rest
type Option func(s *rest)

// WithDatabase set the database client
func WithDatabase(database *database.Database) Option {
	return func(s *rest) {
		s.db = database.DB
		s.rdb = database.RDB
	}
}

// WithCache set the cache client
func WithCache(cache *cache.Cache) Option {
	return func(s *rest) {
		s.cache = cache
	}
}

// NewREST returns a new REST instence
func NewREST(options ...Option) REST {
	s := &rest{}

	for _, opt := range options {
		opt(s)
	}

	return s
}
