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
	CDNTotalCount() (int64, error)
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
