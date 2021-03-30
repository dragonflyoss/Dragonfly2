package dynconfig

import (
	"d7y.io/dragonfly/v2/pkg/cache"
)

type dynconfigLocal struct {
	cache    cache.Cache
	filepath string
}

func newDynconfigLocal(cache cache.Cache, filePath string) *dynconfigLocal {
	d := &dynconfigLocal{
		cache:    cache,
		filepath: filePath,
	}

	// d.load()

	return d
}

func (d *dynconfigLocal) Get() interface{} {
	// cache.GetWithExpiration

	// expire
	// d.load()

	// not expire
	// d.cache.Get()
	return nil
}

func (d *dynconfigLocal) load() error {
	// viper read from file

	// cache.Set file content

	return nil
}
