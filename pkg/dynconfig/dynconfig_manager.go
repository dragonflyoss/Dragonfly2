package dynconfig

import (
	"d7y.io/dragonfly/v2/pkg/cache"
)

const ()

type dynconfigManager struct {
	cache     cache.Cache
	cachePath string
	client    managerClient
}

func newDynconfigManager(cache cache.Cache, cachePath string, client managerClient) *dynconfigManager {
	return &dynconfigManager{
		cache:     cache,
		cachePath: cachePath,
		client:    client,
	}
}

func (d *dynconfigManager) Get() interface{} {
	return nil
}

func (d *dynconfigManager) load() error {
	data, err := d.client.Get()
	if err != nil {
		return err
	}

	cache.Set()

	return nil
}
