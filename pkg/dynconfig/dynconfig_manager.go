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

func defaultCacheDir() (string, error) {
	dir, err := os.UserCacheDir()
	if err != nil {
		return "", err
	}

	return filepath.Join(dir, "dynconfig"), nil
}
