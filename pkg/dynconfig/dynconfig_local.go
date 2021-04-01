/*
 *     Copyright 2020 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dynconfig

import (
	"errors"
	"os"
	"path/filepath"
	"time"

	"d7y.io/dragonfly/v2/pkg/cache"
	"github.com/spf13/viper"
)

type dynconfigLocal struct {
	cache    cache.Cache
	filepath string
	expired  time.Duration
}

// newDynconfigLocal returns a new local dynconfig instence
func newDynconfigLocal(cache cache.Cache, expired time.Duration, filePath string) (*dynconfigLocal, error) {
	d := &dynconfigLocal{
		cache:    cache,
		filepath: filePath,
		expired:  expired,
	}

	if err := d.load(); err != nil {
		return nil, err
	}

	return d, nil
}

// Get dynamic config
func (d *dynconfigLocal) Get() (interface{}, error) {
	// Cache has not expired
	dynconfig, _, found := d.cache.GetWithExpiration(defaultCacheKey)
	if found {
		return dynconfig, nil
	}

	// Cache has expired
	// Reload and ignore client request error
	d.load()

	dynconfig, ok := d.cache.Get(defaultCacheKey)
	if !ok {
		return nil, errors.New("can't find the cached data")
	}

	return dynconfig, nil
}

// Unmarshal unmarshals the config into a Struct. Make sure that the tags
// on the fields of the structure are properly set.
func (d *dynconfigLocal) Unmarshal(rawVal interface{}, opts ...DecoderConfigOption) error {
	dynconfig, err := d.Get()
	if err != nil {
		return errors.New("can't find the cached data")
	}

	return decode(dynconfig, defaultDecoderConfig(rawVal, opts...))
}

// Load dynamic config from local file
func (d *dynconfigLocal) load() error {
	viper.SetConfigFile(d.filepath)

	if err := viper.ReadInConfig(); err != nil {
		return err
	}

	d.cache.Set(defaultCacheKey, viper.AllSettings(), d.expired)
	return nil
}

// Get default config file path
func defaultConfigFile() (string, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}

	return filepath.Join(dir, "dynconfig.yaml"), nil
}
