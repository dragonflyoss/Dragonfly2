package dynconfig

import (
	"errors"
	"os"
	"path/filepath"
	"time"

	"d7y.io/dragonfly/v2/pkg/cache"
)

type SchedulerDynconfig struct {
}

type CDNSystemDynconfig struct {
}

type DfgetFaemonDynconfig struct {
}

type Dynconfig struct {
}

type sourceType string

const (
	// ManagerSourceType represents pulling configuration from manager
	ManagerSourceType sourceType = "manager"

	// LocalSourceType represents read configuration from local file
	LocalSourceType sourceType = "local"
)

// managerClient is a client of manager
type managerClient interface {
	Get() (interface{}, error)
}

type strategy interface {
	Get() interface{}
}

type dynconfig struct {
	sourceType      sourceType
	managerClient   managerClient
	localConfigPath string
	cache           cache.Cache
	cachePath       string
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
		d.strategy = newDynconfigManager(d.cache, d.cachePath, d.managerClient)
	case LocalSourceType:
		d.strategy = newDynconfigLocal(d.cache, d.cachePath, d.localConfigPath)
	default:
		d.strategy = newDynconfigLocal(d.cache, d.cachePath, d.localConfigPath)
	}
	return d, nil
}

// NewDynconfigWithOptions constructs a new instance of a dynconfig with additional options.
func NewDynconfigWithOptions(sourceType sourceType, expire time.Duration, options ...Option) (*dynconfig, error) {
	dir, err := os.UserCacheDir()
	if err != nil {
		return nil, err
	}

	d := &dynconfig{
		sourceType: sourceType,
		cache:      cache.New(expire, cache.NoCleanup),
		cachePath:  filepath.Join(dir, "dynconfig"),
	}

	for _, opt := range options {
		if _, err := opt(d); err != nil {
			return nil, err
		}
	}
	return d, nil
}

func (d *dynconfig) Get() interface{} {
	return d.strategy.Get()
}
