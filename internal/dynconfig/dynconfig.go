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
)

type SourceType string

const (
	// LocalSourceType represents read configuration from local file
	LocalSourceType = "local"

	// ManagerSourceType represents pulling configuration from manager
	ManagerSourceType = "manager"
)

const (
	defaultCacheKey = "dynconfig"
)

type strategy interface {
	Unmarshal(rawVal interface{}) error
}

type Dynconfig struct {
	sourceType      SourceType
	managerClient   ManagerClient
	localConfigPath string
	cachePath       string
	expire          time.Duration
	strategy        strategy
}

// Option is a functional option for configuring the dynconfig
type Option func(d *Dynconfig) error

// WithManagerClient set the manager client
func WithManagerClient(c ManagerClient) Option {
	return func(d *Dynconfig) error {
		d.managerClient = c
		return nil
	}
}

// WithLocalConfigPath set the file path
func WithLocalConfigPath(p string) Option {
	return func(d *Dynconfig) error {
		d.localConfigPath = p
		return nil
	}
}

// WithCachePath set the cache file path
func WithCachePath(p string) Option {
	return func(d *Dynconfig) error {
		d.cachePath = p
		return nil
	}
}

// WithExpireTime set the expire time for cache
func WithExpireTime(e time.Duration) Option {
	return func(d *Dynconfig) error {
		d.expire = e
		return nil
	}
}

// New returns a new dynconfig instance
func New(sourceType SourceType, options ...Option) (*Dynconfig, error) {
	d, err := NewDynconfigWithOptions(sourceType, options...)
	if err != nil {
		return nil, err
	}

	if err := d.validate(); err != nil {
		return nil, err
	}

	switch sourceType {
	case ManagerSourceType:
		d.strategy, err = newDynconfigManager(d.expire, d.cachePath, d.managerClient)
		if err != nil {
			return nil, err
		}
	case LocalSourceType:
		d.strategy, err = newDynconfigLocal(d.localConfigPath)
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("unknown source type")
	}

	return d, nil
}

// NewDynconfigWithOptions constructs a new instance of a dynconfig with additional options.
func NewDynconfigWithOptions(sourceType SourceType, options ...Option) (*Dynconfig, error) {
	d := &Dynconfig{
		sourceType: sourceType,
	}

	for _, opt := range options {
		if err := opt(d); err != nil {
			return nil, err
		}
	}

	return d, nil
}

// validate parameters
func (d *Dynconfig) validate() error {
	if d.sourceType == ManagerSourceType {
		if d.managerClient == nil {
			return errors.New("missing parameter ManagerClient, use method WithManagerClient to assign")
		}
		if d.cachePath == "" {
			return errors.New("missing parameter CachePath, use method WithCachePath to assign")
		}
		if d.expire == 0 {
			return errors.New("missing parameter Expire, use method WithExpireTime to assign")
		}
	}

	if d.sourceType == LocalSourceType && d.localConfigPath == "" {
		return errors.New("missing parameter LocalConfigPath, use method WithLocalConfigPath to assign")
	}

	return nil
}

// Unmarshal unmarshals the config into a Struct. Make sure that the tags
// on the fields of the structure are properly set.
func (d *Dynconfig) Unmarshal(rawVal interface{}) error {
	return d.strategy.Unmarshal(rawVal)
}

// A DecoderConfigOption can be passed to dynconfig Unmarshal to configure
// mapstructure.DecoderConfig options
type DecoderConfigOption func(*mapstructure.DecoderConfig)

// defaultDecoderConfig returns default mapstructure.DecoderConfig with support
// of time.Duration values & string slices
func defaultDecoderConfig(output interface{}) *mapstructure.DecoderConfig {
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

// A wrapper around mapstructure.Decode that mimics the WeakDecode functionality
func decode(input interface{}, config *mapstructure.DecoderConfig) error {
	decoder, err := mapstructure.NewDecoder(config)
	if err != nil {
		return err
	}
	return decoder.Decode(input)
}
