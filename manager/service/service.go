package service

import (
	"d7y.io/dragonfly/v2/manager/database"
)

type Service interface {
}

type service struct {
	db *database.Database
}

// Option is a functional option for service
type Option func(s *service)

// WithDatabase set the database client
func WithDatabase(db *database.Database) Option {
	return func(s *service) {
		s.db = db
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
