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
	"time"

	"github.com/mitchellh/mapstructure"

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

type Dynconfig interface {
	// Unmarshal unmarshals the config into a Struct. Make sure that the tags
	// on the fields of the structure are properly set.
	Unmarshal(rawVal any) error

	// Refresh refreshes dynconfig in cache.
	Refresh() error
}

type dynconfig struct {
	cache     cache.Cache
	client    ManagerClient
	cachePath string
	expire    time.Duration
}

// New returns a new dynconfig instance.
func New(client ManagerClient, cachePath string, expire time.Duration) (Dynconfig, error) {
	d := &dynconfig{
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

// Unmarshal unmarshals the config into a Struct. Make sure that the tags
// on the fields of the structure are properly set.
func (d *dynconfig) Unmarshal(rawVal any) error {
	dynconfig, err := d.get()
	if err != nil {
		return errors.New("can't find the cached data")
	}

	return decode(dynconfig, defaultDecoderConfig(rawVal))
}

// Refresh refreshes dynconfig in cache.
func (d *dynconfig) Refresh() error {
	return d.load()
}

// Get dynamic config.
func (d *dynconfig) get() (any, error) {
	// Cache has not expired.
	dynconfig, _, found := d.cache.GetWithExpiration(defaultCacheKey)
	if found {
		return dynconfig, nil
	}

	// Cache has expired, refresh and ignore client request error.
	if err := d.load(); err != nil {
		logger.Warn("reload failed ", err)
	}

	dynconfig, ok := d.cache.Get(defaultCacheKey)
	if !ok {
		return nil, errors.New("can't find the cached data")
	}

	return dynconfig, nil
}

// Load dynamic config from manager.
func (d *dynconfig) load() error {
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

// A DecoderConfigOption can be passed to dynconfig Unmarshal to configure
// mapstructure.DecoderConfig options.
type DecoderConfigOption func(*mapstructure.DecoderConfig)

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
