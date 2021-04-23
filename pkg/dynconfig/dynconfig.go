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

	"d7y.io/dragonfly/v2/pkg/cache"
	"github.com/mitchellh/mapstructure"
)

type sourceType int

const (
	// ManagerSourceType represents pulling configuration from manager
	ManagerSourceType sourceType = 1 << iota

	// LocalSourceType represents read configuration from local file
	LocalSourceType
)

const (
	defaultCacheKey = "dynconfig"
)

type strategy interface {
	Get() (interface{}, error)
	Unmarshal(rawVal interface{}, opts ...DecoderConfigOption) error
}

type dynconfig struct {
	sourceType      sourceType
	managerClient   managerClient
	localConfigPath string
	cache           cache.Cache
	expire          time.Duration
	strategy        strategy
}

// Option is a functional option for configuring the dynconfig
type Option func(d *dynconfig) (*dynconfig, error)

// WithManagerClient set the manager client
func WithManagerClient(c managerClient) Option {
	return func(d *dynconfig) (*dynconfig, error) {
		if d.sourceType != ManagerSourceType {
			return nil, errors.New("the source type must be ManagerSourceType")
		}

		d.managerClient = c
		return d, nil
	}
}

// WithManagerClient set the file path
func WithLocalConfigPath(p string) Option {
	return func(d *dynconfig) (*dynconfig, error) {
		if d.sourceType != LocalSourceType {
			return nil, errors.New("the source type must be LocalSourceType")
		}

		d.localConfigPath = p
		return d, nil
	}
}

// NewDynconfig returns a new dynconfig instence
func New(sourceType sourceType, expire time.Duration, options ...Option) (*dynconfig, error) {
	d, err := NewDynconfigWithOptions(sourceType, expire, options...)
	if err != nil {
		return nil, err
	}

	switch sourceType {
	case ManagerSourceType:
		d.strategy, err = newDynconfigManager(d.cache, expire, d.managerClient)
		if err != nil {
			return nil, err
		}
	case LocalSourceType:
		d.strategy, err = newDynconfigLocal(d.cache, expire, d.localConfigPath)
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("unknown source type")
	}

	return d, nil
}

// NewDynconfigWithOptions constructs a new instance of a dynconfig with additional options.
func NewDynconfigWithOptions(sourceType sourceType, expire time.Duration, options ...Option) (*dynconfig, error) {
	d := &dynconfig{
		sourceType: sourceType,
		cache:      cache.New(expire, cache.NoCleanup),
		expire:     expire,
	}

	for _, opt := range options {
		if _, err := opt(d); err != nil {
			return nil, err
		}
	}
	return d, nil
}

// Get dynamic config
func (d *dynconfig) Get() (interface{}, error) {
	return d.strategy.Get()
}

// Unmarshal unmarshals the config into a Struct. Make sure that the tags
// on the fields of the structure are properly set.
func (d *dynconfig) Unmarshal(rawVal interface{}, opts ...DecoderConfigOption) error {
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
