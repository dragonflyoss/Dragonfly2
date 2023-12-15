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

//go:generate mockgen -destination mocks/dynconfig_mock.go -source dynconfig.go -package mocks

package dynconfig

import (
	"errors"
	"sync"
	"time"

	"github.com/mitchellh/mapstructure"
	"go.uber.org/atomic"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/cache"
)

type SourceType string

const (
	// CacheDirName is dir name of dynconfig cache.
	CacheDirName = "dynconfig"
)

const (
	// defaultCacheKey represents cache key of dynconfig.
	defaultCacheKey = "dynconfig"
)

type Dynconfig[T any] interface {
	// Get raw dynamic config.
	Get() (*T, error)

	// Refresh refreshes dynconfig in cache.
	Refresh() error
}

type dynconfig[T any] struct {
	cache     cache.Cache
	client    ManagerClient
	cachePath string
	data      *atomic.Pointer[T]
	expire    time.Duration
	mu        *sync.Mutex
}

// New returns a new dynconfig instance.
func New[T any](client ManagerClient, cachePath string, expire time.Duration) (Dynconfig[T], error) {
	d := &dynconfig[T]{
		cache:     cache.New(expire, cache.NoCleanup),
		cachePath: cachePath,
		data:      atomic.NewPointer[T](nil),
		expire:    expire,
		client:    client,
		mu:        &sync.Mutex{},
	}

	if err := d.load(); err != nil {
		return nil, err
	}

	return d, nil
}

// Refresh refreshes dynconfig in cache.
func (d *dynconfig[T]) Refresh() error {
	return d.load()
}

// Get dynamic config.
func (d *dynconfig[T]) Get() (*T, error) {
	// If load is abnormal, data can be nil.
	if d.data.Load() == nil {
		return nil, errors.New("invalid data")
	}

	// Cache has not expired.
	if _, _, found := d.cache.GetWithExpiration(defaultCacheKey); found {
		return d.data.Load(), nil
	}

	// Cache has expired, refresh and ignore client request error.
	if err := d.load(); err != nil {
		logger.Warn("reload failed ", err)
	}

	if _, found := d.cache.Get(defaultCacheKey); found {
		return d.data.Load(), nil
	}

	return nil, errors.New("cache not found")
}

// Load dynamic config from manager.
func (d *dynconfig[T]) load() error {
	// If another load is in progress, return directly.
	if !d.mu.TryLock() {
		return errors.New("load is running")
	}
	defer d.mu.Unlock()

	rawData, err := d.client.Get()
	if err != nil {
		return err
	}

	var data T
	if err := decode(rawData, defaultDecoderConfig(&data)); err != nil {
		return err
	}
	d.data.Store(&data)

	d.cache.Set(defaultCacheKey, rawData, d.expire)
	if err := d.cache.SaveFile(d.cachePath); err != nil {
		return err
	}

	return nil
}

// defaultDecoderConfig returns default mapstructure.DecoderConfig with support
// of time.Duration values & string slices.
func defaultDecoderConfig(output any) *mapstructure.DecoderConfig {
	c := &mapstructure.DecoderConfig{
		Metadata:         nil,
		Result:           output,
		WeaklyTypedInput: true,
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			mapstructure.StringToTimeDurationHookFunc(),
			mapstructure.StringToSliceHookFunc(","),
		),
	}
	return c
}

// A wrapper around mapstructure.Decode that mimics the WeakDecode functionality.
func decode(input any, config *mapstructure.DecoderConfig) error {
	decoder, err := mapstructure.NewDecoder(config)
	if err != nil {
		return err
	}
	return decoder.Decode(input)
}
