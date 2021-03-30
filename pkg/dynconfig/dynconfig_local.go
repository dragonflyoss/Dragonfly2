package dynconfig

import (
	"d7y.io/dragonfly/v2/pkg/cache"
)

type dynconfigLocal struct {
	cache     cache.Cache
	cachePath string
	filepath  string
}

func newDynconfigLocal(cache cache.Cache, cachePath, filePath string) *dynconfigLocal {
	return &dynconfigLocal{
		cache:     cache,
		cachePath: cachePath,
		filepath:  filePath,
	}
}

func (d *dynconfigLocal) Get() interface{} {
	return nil
}

func (d *dynconfigLocal) load() error {
	if err := d.cache.LoadFile(d.filepath); err != nil {
		return err
	}

	return nil
}
