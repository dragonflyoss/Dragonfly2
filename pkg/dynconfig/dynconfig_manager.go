package dynconfig

import (
	"errors"
	"os"
	"path/filepath"

	"d7y.io/dragonfly/v2/pkg/cache"
)

const ()

type dynconfigManager struct {
	cache     cache.Cache
	cachePath string
	client    managerClient
}

// newDynconfigManager returns a new manager dynconfig instence
func newDynconfigManager(cache cache.Cache, client managerClient) (*dynconfigManager, error) {
	cachePath, err := defaultCacheDir()
	if err != nil {
		return nil, err
	}

	d := &dynconfigManager{
		cache:     cache,
		cachePath: cachePath,
		client:    client,
	}

	if err := d.cache.LoadFile(d.cachePath); err != nil {
		if err := d.load(); err != nil {
			return nil, err
		}
	}

	return d, nil
}

// Get dynamic config
func (d *dynconfigManager) Get() (interface{}, error) {
	// Cache has not expired
	dynconfig, _, found := d.cache.GetWithExpiration(defaultCacheKey)
	if found {
		return dynconfig, nil
	}

	// Cache has expired
	if err := d.load(); err != nil {
		return nil, err
	}

	dynconfig, ok := d.cache.Get(defaultCacheKey)
	if !ok {
		return nil, errors.New("can't find the cached data")
	}

	return dynconfig, nil
}

// Unmarshal unmarshals the config into a Struct. Make sure that the tags
// on the fields of the structure are properly set.
func (d *dynconfigManager) Unmarshal(rawVal interface{}, opts ...DecoderConfigOption) error {
	dynconfig, err := d.Get()
	if err != nil {
		return errors.New("can't find the cached data")
	}

	return decode(dynconfig, defaultDecoderConfig(rawVal, opts...))
}

// Load dynamic config from manager
func (d *dynconfigManager) load() error {
	dynconfig, err := d.client.Get()
	if err != nil {
		return err
	}

	d.cache.SetDefault(defaultCacheKey, dynconfig)
	if err := d.cache.SaveFile(d.cachePath); err != nil {
		return err
	}
	return nil
}

// Get default cache directory
func defaultCacheDir() (string, error) {
	dir, err := os.UserCacheDir()
	if err != nil {
		return "", err
	}

	return filepath.Join(dir, "dynconfig"), nil
}
