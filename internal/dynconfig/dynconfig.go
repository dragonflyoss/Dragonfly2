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

	"github.com/mitchellh/mapstructure"
)

type SourceType int

const (
	// LocalSourceType represents read configuration from local file
	LocalSourceType SourceType = iota

	// ManagerSourceType represents pulling configuration from manager
	ManagerSourceType
)

const (
	defaultCacheKey = "dynconfig"
)

type strategy interface {
	Unmarshal(rawVal interface{}, opts ...DecoderConfigOption) error
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
type Option func(d *Dynconfig) (*Dynconfig, error)

// WithManagerClient set the manager client
func WithManagerClient(c ManagerClient) Option {
	return func(d *Dynconfig) (*Dynconfig, error) {
		if d.sourceType != ManagerSourceType {
			return nil, errors.New("the source type must be ManagerSourceType")
		}

		d.managerClient = c
		return d, nil
	}
}

// WithLocalConfigPath set the file path
func WithLocalConfigPath(p string) Option {
	return func(d *Dynconfig) (*Dynconfig, error) {
		if d.sourceType != LocalSourceType {
			return nil, errors.New("the source type must be LocalSourceType")
		}

		d.localConfigPath = p
		return d, nil
	}
}

// WithCachePath set the cache file path
func WithCachePath(p string) Option {
	return func(d *Dynconfig) (*Dynconfig, error) {
		if d.sourceType != ManagerSourceType {
			return nil, errors.New("the source type must be ManagerSourceType")
		}

		d.cachePath = p
		return d, nil
	}
}

// WithExpireTime set the expire time for cache
func WithExpireTime(e time.Duration) Option {
	return func(d *Dynconfig) (*Dynconfig, error) {
		if d.sourceType != ManagerSourceType {
			return nil, errors.New("the source type must be ManagerSourceType")
		}

		d.expire = e
		return d, nil
	}
}

// NewDynconfig returns a new dynconfig instence
func New(sourceType SourceType, options ...Option) (*Dynconfig, error) {
	d, err := NewDynconfigWithOptions(sourceType, options...)
	if err != nil {
		return nil, err
	}

	if err := d.Validate(); err != nil {
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
		if _, err := opt(d); err != nil {
			return nil, err
		}
	}

	return d, nil
}

// Validate parameters
func (d *Dynconfig) Validate() error {
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
func (d *Dynconfig) Unmarshal(rawVal interface{}, opts ...DecoderConfigOption) error {
	return d.strategy.Unmarshal(rawVal, opts...)
}

// A DecoderConfigOption can be passed to dynconfig Unmarshal to configure
// mapstructure.DecoderConfig options
type DecoderConfigOption func(*mapstructure.DecoderConfig)

// defaultDecoderConfig returns default mapsstructure.DecoderConfig with suppot
// of time.Duration values & string slices
func defaultDecoderConfig(output interface{}, opts ...DecoderConfigOption) *mapstructure.DecoderConfig {
	c := &mapstructure.DecoderConfig{
		Metadata:         nil,
		Result:           output,
		WeaklyTypedInput: true,
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			mapstructure.StringToTimeDurationHookFunc(),
			mapstructure.StringToSliceHookFunc(","),
		),
	}
	for _, opt := range opts {
		opt(c)
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
