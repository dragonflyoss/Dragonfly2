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

package storedriver

import (
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"d7y.io/dragonfly/v2/cdnsystem/plugins"
	"d7y.io/dragonfly/v2/pkg/unit"
	"d7y.io/dragonfly/v2/pkg/util/fileutils"
	"github.com/mitchellh/mapstructure"
	"gopkg.in/yaml.v3"
)

// DriverBuilder is a function that creates a new storage driver plugin instant with the giving Config.
type DriverBuilder func(cfg *Config) (Driver, error)

// Register defines an interface to register a driver with specified name.
// All drivers should call this function to register itself to the driverFactory.
func Register(name string, builder DriverBuilder) {
	name = strings.ToLower(name)
	// plugin builder
	var f = func(conf interface{}) (plugins.Plugin, error) {
		cfg := &Config{}
		decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
			DecodeHook: mapstructure.ComposeDecodeHookFunc(func(from, to reflect.Type, v interface{}) (interface{}, error) {
				switch to {
				case reflect.TypeOf(unit.B),
					reflect.TypeOf(time.Second):
					b, _ := yaml.Marshal(v)
					p := reflect.New(to)
					if err := yaml.Unmarshal(b, p.Interface()); err != nil {
						return nil, err
					}
					return p.Interface(), nil
				default:
					return v, nil
				}
			}),
			Result: cfg,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create decoder: %v", err)
		}
		err = decoder.Decode(conf)
		if err != nil {
			return nil, fmt.Errorf("failed to parse config: %v", err)
		}
		// prepare the base dir
		if !filepath.IsAbs(cfg.BaseDir) {
			return nil, fmt.Errorf("not absolute path: %s", cfg.BaseDir)
		}
		if err := fileutils.MkdirAll(cfg.BaseDir); err != nil {
			return nil, fmt.Errorf("failed to create baseDir%s: %v", cfg.BaseDir, err)
		}

		return newDriverPlugin(name, builder, cfg)
	}
	plugins.RegisterPluginBuilder(plugins.StorageDriverPlugin, name, f)
}

// Get a store from manager with specified name.
func Get(name string) (Driver, error) {
	v := plugins.GetPlugin(plugins.StorageDriverPlugin, strings.ToLower(name))
	if v == nil {
		return nil, fmt.Errorf("storage: %s not existed", name)
	}
	if plugin, ok := v.(*driverPlugin); ok {
		return plugin.instance, nil
	}
	return nil, fmt.Errorf("get store driver %s error: unknown reason", name)
}
