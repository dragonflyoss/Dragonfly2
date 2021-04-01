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
)

const ()

type dynconfigManager struct {
	cache     cache.Cache
	cachePath string
	expire    time.Duration
	client    managerClient
}

// newDynconfigManager returns a new manager dynconfig instence
func newDynconfigManager(cache cache.Cache, expire time.Duration, client managerClient) (*dynconfigManager, error) {
	cachePath, err := defaultCacheDir()
	if err != nil {
		return nil, err
	}

	d := &dynconfigManager{
		cache:     cache,
		cachePath: cachePath,
		expire:    expire,
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

	d.cache.Set(defaultCacheKey, dynconfig, d.expire)
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
