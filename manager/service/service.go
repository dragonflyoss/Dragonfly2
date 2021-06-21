package service

import (
	"d7y.io/dragonfly/v2/manager/database"
	"d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/manager/types"
	rdbcache "github.com/go-redis/cache/v8"
	"github.com/go-redis/redis/v8"
	"gorm.io/gorm"
)

type Service interface {
	CreateCDN(types.CreateCDNRequest) (*model.CDN, error)
	DestroyCDN(string) error
	UpdateCDN(string, types.UpdateCDNRequest) (*model.CDN, error)
	GetCDN(string) (*model.CDN, error)
	GetCDNs(types.GetCDNsQuery) (*[]model.CDN, error)
	CDNTotalCount(types.GetCDNsQuery) (int64, error)
	AddInstanceToCDN(string, string) error
	AddSchedulerToCDN(string, string) error

	CreateCDNInstance(types.CreateCDNInstanceRequest) (*model.CDNInstance, error)
	CreateCDNInstanceWithSecurityGroupDomain(types.CreateCDNInstanceRequest) (*model.CDNInstance, error)
	DestroyCDNInstance(string) error
	UpdateCDNInstance(string, types.UpdateCDNInstanceRequest) (*model.CDNInstance, error)
	UpdateCDNInstanceWithSecurityGroupDomain(string, types.UpdateCDNInstanceRequest) (*model.CDNInstance, error)
	GetCDNInstance(string) (*model.CDNInstance, error)
	GetCDNInstances(types.GetCDNInstancesQuery) (*[]model.CDNInstance, error)
	CDNInstanceTotalCount(types.GetCDNInstancesQuery) (int64, error)

	CreateScheduler(types.CreateSchedulerRequest) (*model.Scheduler, error)
	DestroyScheduler(string) error
	UpdateScheduler(string, types.UpdateSchedulerRequest) (*model.Scheduler, error)
	GetScheduler(string) (*model.Scheduler, error)
	GetSchedulers(types.GetSchedulersQuery) (*[]model.Scheduler, error)
	SchedulerTotalCount(types.GetSchedulersQuery) (int64, error)
	AddInstanceToScheduler(string, string) error

	CreateSchedulerInstance(types.CreateSchedulerInstanceRequest) (*model.SchedulerInstance, error)
	CreateSchedulerInstanceWithSecurityGroupDomain(types.CreateSchedulerInstanceRequest) (*model.SchedulerInstance, error)
	DestroySchedulerInstance(string) error
	UpdateSchedulerInstance(string, types.UpdateSchedulerInstanceRequest) (*model.SchedulerInstance, error)
	UpdateSchedulerInstanceWithSecurityGroupDomain(string, types.UpdateSchedulerInstanceRequest) (*model.SchedulerInstance, error)
	GetSchedulerInstance(string) (*model.SchedulerInstance, error)
	GetSchedulerInstances(types.GetSchedulerInstancesQuery) (*[]model.SchedulerInstance, error)
	SchedulerInstanceTotalCount(types.GetSchedulerInstancesQuery) (int64, error)

	CreateSecurityGroup(types.CreateSecurityGroupRequest) (*model.SecurityGroup, error)
	DestroySecurityGroup(string) error
	UpdateSecurityGroup(string, types.UpdateSecurityGroupRequest) (*model.SecurityGroup, error)
	GetSecurityGroup(string) (*model.SecurityGroup, error)
	GetSecurityGroups(types.GetSecurityGroupsQuery) (*[]model.SecurityGroup, error)
	SecurityGroupTotalCount(types.GetSecurityGroupsQuery) (int64, error)
	AddSchedulerInstanceToSecurityGroup(string, string) error
	AddCDNInstanceToSecurityGroup(string, string) error
}

type service struct {
	db    *gorm.DB
	rdb   *redis.Client
	cache *rdbcache.Cache
}

// Option is a functional option for service
type Option func(s *service)

// WithDatabase set the database client
func WithDatabase(database *database.Database) Option {
	return func(s *service) {
		s.db = database.DB
		s.rdb = database.RDB
		s.cache = database.Cache
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
