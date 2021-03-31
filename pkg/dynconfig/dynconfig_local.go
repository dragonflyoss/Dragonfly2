package dynconfig

import (
	"errors"

	"d7y.io/dragonfly/v2/pkg/cache"
	"github.com/spf13/viper"
)

type dynconfigLocal struct {
	cache    cache.Cache
	filepath string
}

func newDynconfigLocal(cache cache.Cache, filePath string) (*dynconfigLocal, error) {
	d := &dynconfigLocal{
		cache:    cache,
		filepath: filePath,
	}

	if err := d.load(); err != nil {
		return nil, err
	}

	return d, nil
}

func (d *dynconfigLocal) Get() (interface{}, error) {
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

func (d *dynconfigLocal) load() error {
	viper.SetConfigFile(d.filepath)

	if err := viper.ReadInConfig(); err != nil {
		return err
	}

	d.cache.SetDefault(defaultCacheKey, viper.AllSettings())
	return nil
}
