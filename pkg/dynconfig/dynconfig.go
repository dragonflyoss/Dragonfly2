package dynconfig

import (
	"errors"
	"time"

	"d7y.io/dragonfly/v2/pkg/cache"
)

type sourceType string

const (
	// ManagerSourceType represents pulling configuration from manager
	ManagerSourceType sourceType = "manager"

	// LocalSourceType represents read configuration from local file
	LocalSourceType sourceType = "local"
)

const (
	defaultCacheKey = "dynconfig"
)

// managerClient is a client of manager
type managerClient interface {
	Get() (interface{}, error)
}

type strategy interface {
	Get() (interface{}, error)
}

type dynconfig struct {
	sourceType      sourceType
	managerClient   managerClient
	localConfigPath string
	cache           cache.Cache
	strategy        strategy
}

// Option is a functional option for configuring the dynconfig
type Option func(d *dynconfig) (*dynconfig, error)

// WithManagerClient set the manager client
func WithManagerClient(c managerClient) Option {
	return func(d *dynconfig) (*dynconfig, error) {
		if d.sourceType != ManagerSourceType {
			return nil, errors.New("the source type must be ManagerSourceType")
		}

		d.managerClient = c
		return d, nil
	}
}

// WithManagerClient set the file path
func WithLocalConfigPath(p string) Option {
	return func(d *dynconfig) (*dynconfig, error) {
		if d.sourceType != LocalSourceType {
			return nil, errors.New("the source type must be LocalSourceType")
		}

		d.localConfigPath = p
		return d, nil
	}
}

// NewDynconfig returns a new dynconfig instence
func NewDynconfig(sourceType sourceType, expire time.Duration, options ...Option) (*dynconfig, error) {
	d, err := NewDynconfigWithOptions(sourceType, expire, options...)
	if err != nil {
		return nil, err
	}

	switch sourceType {
	case ManagerSourceType:
		d.strategy, err = newDynconfigManager(d.cache, d.managerClient)
		if err != nil {
			return nil, err
		}
	case LocalSourceType:
		d.strategy, err = newDynconfigLocal(d.cache, d.localConfigPath)
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("unknown source type")
	}

	return d, nil
}

// NewDynconfigWithOptions constructs a new instance of a dynconfig with additional options.
func NewDynconfigWithOptions(sourceType sourceType, expire time.Duration, options ...Option) (*dynconfig, error) {
	d := &dynconfig{
		sourceType: sourceType,
		cache:      cache.New(expire, cache.NoCleanup),
	}

	for _, opt := range options {
		if _, err := opt(d); err != nil {
			return nil, err
		}
	}
	return d, nil
}

func (d *dynconfig) Get() (interface{}, error) {
	return d.strategy.Get()
}
