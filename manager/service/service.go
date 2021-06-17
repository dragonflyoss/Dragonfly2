package service

import (
	"d7y.io/dragonfly/v2/manager/cache"
	"d7y.io/dragonfly/v2/manager/database"
	"d7y.io/dragonfly/v2/manager/lease"
)

type Service interface {
}

type service struct {
	db     *database.Database
	cache  *cache.Cache
	lessor lease.Lessor
}

// Option is a functional option for service
type Option func(s *service)

// WithDatabase set the database client
func WithDatabase(db *database.Database) Option {
	return func(s *service) {
		s.db = db
	}
}

// WithCache set the cache client
func WithCache(c *cache.Cache) Option {
	return func(s *service) {
		s.cache = c
	}
}

// WithLessor set the lessor manager
func WithLessor(l lease.Lessor) Option {
	return func(s *service) {
		s.lessor = l
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
