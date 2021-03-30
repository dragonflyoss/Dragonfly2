package dynconfig

import (
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

	// d.load()

	// d.cache.SaveFile()

	return d, nil
}

func (d *dynconfigManager) Get() interface{} {
	// cache.GetWithExpiration

	// expire
	// d.load()

	// not expire
	// d.cache.Get()
	return nil
}

func (d *dynconfigManager) load() error {
	// client.Get config from manager

	// cache.Set config

	return nil
}

func defaultCacheDir() (string, error) {
	dir, err := os.UserCacheDir()
	if err != nil {
		return "", err
	}

	return filepath.Join(dir, "dynconfig"), nil
}
