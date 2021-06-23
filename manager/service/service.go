package service

import (
	"d7y.io/dragonfly/v2/manager/cache"
	"d7y.io/dragonfly/v2/manager/database"
	"d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/manager/types"
)

type Service interface {
	CreateCDNCluster(types.CreateCDNClusterRequest) (*model.CDNCluster, error)
	DestroyCDNCluster(uint) error
	UpdateCDNCluster(uint, types.UpdateCDNClusterRequest) (*model.CDNCluster, error)
	GetCDNCluster(uint) (*model.CDNCluster, error)
	GetCDNClusters(types.GetCDNClustersQuery) (*[]model.CDNCluster, error)
	CDNClusterTotalCount(types.GetCDNClustersQuery) (int64, error)
	AddCDNToCDNCluster(uint, uint) error
	AddSchedulerClusterToCDNCluster(uint, uint) error

	CreateCDN(types.CreateCDNRequest) (*model.CDN, error)
	CreateCDNWithSecurityGroupDomain(types.CreateCDNRequest) (*model.CDN, error)
	DestroyCDN(uint) error
	UpdateCDN(uint, types.UpdateCDNRequest) (*model.CDN, error)
	UpdateCDNWithSecurityGroupDomain(uint, types.UpdateCDNRequest) (*model.CDN, error)
	GetCDN(uint) (*model.CDN, error)
	GetCDNs(types.GetCDNsQuery) (*[]model.CDN, error)
	CDNTotalCount(types.GetCDNsQuery) (int64, error)

	CreateSchedulerCluster(types.CreateSchedulerClusterRequest) (*model.SchedulerCluster, error)
	DestroySchedulerCluster(uint) error
	UpdateSchedulerCluster(uint, types.UpdateSchedulerClusterRequest) (*model.SchedulerCluster, error)
	GetSchedulerCluster(uint) (*model.SchedulerCluster, error)
	GetSchedulerClusters(types.GetSchedulerClustersQuery) (*[]model.SchedulerCluster, error)
	SchedulerClusterTotalCount(types.GetSchedulerClustersQuery) (int64, error)
	AddSchedulerToSchedulerCluster(uint, uint) error

	CreateScheduler(types.CreateSchedulerRequest) (*model.Scheduler, error)
	CreateSchedulerWithSecurityGroupDomain(types.CreateSchedulerRequest) (*model.Scheduler, error)
	DestroyScheduler(uint) error
	UpdateScheduler(uint, types.UpdateSchedulerRequest) (*model.Scheduler, error)
	UpdateSchedulerWithSecurityGroupDomain(uint, types.UpdateSchedulerRequest) (*model.Scheduler, error)
	GetScheduler(uint) (*model.Scheduler, error)
	GetSchedulers(types.GetSchedulersQuery) (*[]model.Scheduler, error)
	SchedulerTotalCount(types.GetSchedulersQuery) (int64, error)

	CreateSecurityGroup(types.CreateSecurityGroupRequest) (*model.SecurityGroup, error)
	DestroySecurityGroup(uint) error
	UpdateSecurityGroup(uint, types.UpdateSecurityGroupRequest) (*model.SecurityGroup, error)
	GetSecurityGroup(uint) (*model.SecurityGroup, error)
	GetSecurityGroups(types.GetSecurityGroupsQuery) (*[]model.SecurityGroup, error)
	SecurityGroupTotalCount(types.GetSecurityGroupsQuery) (int64, error)
	AddSchedulerToSecurityGroup(uint, uint) error
	AddCDNToSecurityGroup(uint, uint) error
}

type service struct {
	db    *database.Database
	cache *cache.Cache
}

// Option is a functional option for service
type Option func(s *service)

// WithDatabase set the database client
func WithDatabase(database *database.Database) Option {
	return func(s *service) {
		s.db = database
	}
}

// WithCache set the cache client
func WithCache(cache *cache.Cache) Option {
	return func(s *service) {
		s.cache = cache
	}
}

// New returns a new Service instence
func New(options ...Option) Service {
	s := &service{}

	for _, opt := range options {
		opt(s)
	}

	return s
}
