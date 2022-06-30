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
	"time"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/cache"
)

type dynconfigManager struct {
	cachePath string
	cache     cache.Cache
	expire    time.Duration
	client    ManagerClient
}

// newDynconfigManager returns a new manager dynconfig instence
func newDynconfigManager(expire time.Duration, cachePath string, client ManagerClient) (*dynconfigManager, error) {
	d := &dynconfigManager{
		cache:     cache.New(expire, cache.NoCleanup),
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
func (d *dynconfigManager) get() (any, error) {
	// Cache has not expired
	dynconfig, _, found := d.cache.GetWithExpiration(defaultCacheKey)
	if found {
		return dynconfig, nil
	}

	// Cache has expired
	// Reload and ignore client request error
	if err := d.load(); err != nil {
		logger.Warn("reload failed ", err)
	}

	dynconfig, ok := d.cache.Get(defaultCacheKey)
	if !ok {
		return nil, errors.New("can't find the cached data")
	}

	return dynconfig, nil
}

// Unmarshal unmarshals the config into a Struct. Make sure that the tags
// on the fields of the structure are properly set.
func (d *dynconfigManager) Unmarshal(rawVal any) error {
	dynconfig, err := d.get()
	if err != nil {
		return errors.New("can't find the cached data")
	}

	return decode(dynconfig, defaultDecoderConfig(rawVal))
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
