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

package config

import (
	"d7y.io/dragonfly/v2/pkg/ratelimiter"
	"fmt"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"time"
)

// NewConfig creates an instant with default values.
func NewConfig() *Config {
	return &Config{
		BaseProperties: NewBaseProperties(),
		Plugins:        NewPlugins(),
	}
}

// Config contains all configuration of cdnNode.
type Config struct {
	*BaseProperties `yaml:"base"`
	Plugins         map[PluginType][]*PluginProperties `yaml:"plugins"`
}

// Load loads config properties from the giving file.
func (c *Config) Load(path string) error {
	content, err := ioutil.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to load yaml %s when reading file: %v", path, err)
	}

	if err = yaml.Unmarshal(content, c); err != nil {
		return fmt.Errorf("failed to load yaml %s: %v", path, err)
	}

	return nil
}

func (c *Config) String() string {
	if out, err := yaml.Marshal(c); err == nil {
		return string(out)
	}
	return ""
}

func NewPlugins() map[PluginType][]*PluginProperties {
	// todo 创建默认plugins
	return map[PluginType][]*PluginProperties{
		StoragePlugin: {
			{
				Name:   "disk",
				Enable: true,
				Config: map[string]interface{}{
					"baseDir": "/tmp/cdnsystem",
					"gcConfig": map[string]interface{}{
						"youngGCThreshold":  "100G",
						"fullGCThreshold":   "5G",
						"cleanRatio":        1,
						"intervalThreshold": "2h",
					},
				},
			}, {
				Name:   "memory",
				Enable: true,
				Config: map[string]interface{}{
					"baseDir": "/tmp/memory/dragonfly",
					"gcConfig": map[string]interface{}{
						"youngGCThreshold":  "100G",
						"fullGCThreshold":   "5G",
						"cleanRatio":        3,
						"intervalThreshold": "2h",
					},
				},
			},
		},
	}
}

// NewBaseProperties creates an instant with default values.
func NewBaseProperties() *BaseProperties {
	return &BaseProperties{
		ListenPort:              DefaultListenPort,
		DownloadPort:            DefaultDownloadPort,
		SystemReservedBandwidth: DefaultSystemReservedBandwidth,
		MaxBandwidth:            DefaultMaxBandwidth,
		EnableProfiler:          false,
		FailAccessInterval:      DefaultFailAccessInterval,
		GCInitialDelay:          DefaultGCInitialDelay,
		GCMetaInterval:          DefaultGCMetaInterval,
		GCStorageInterval:       DefaultGCStorageInterval,
		TaskExpireTime:          DefaultTaskExpireTime,
		StoragePattern:          DefaultStoragePattern,
		Console:                 DefaultConsole,
	}
}

// BaseProperties contains all basic properties of cdn system.
type BaseProperties struct {

	// ListenPort is the port cdn server listens on.
	// default: 8002
	ListenPort int `yaml:"listenPort"`

	// DownloadPort is the port for download files from cdn.
	// default: 8001
	DownloadPort int `yaml:"downloadPort"`

	// SystemReservedBandwidth is the network bandwidth reserved for system software.
	// default: 20 MB, in format of G(B)/g/M(B)/m/K(B)/k/B, pure number will also be parsed as Byte.
	SystemReservedBandwidth ratelimiter.Rate `yaml:"systemReservedBandwidth"`

	// MaxBandwidth is the network bandwidth that cdn system can use.
	// default: 200 MB, in format of G(B)/g/M(B)/m/K(B)/k/B, pure number will also be parsed as Byte.
	MaxBandwidth ratelimiter.Rate `yaml:"maxBandwidth"`

	// Whether to enable profiler
	// default: false
	EnableProfiler bool `yaml:"enableProfiler"`

	// AdvertiseIP is used to set the ip that we advertise to other peer in the p2p-network.
	// By default, the first non-loop address is advertised.
	AdvertiseIP string `yaml:"advertiseIP"`

	// FailAccessInterval is the interval time after failed to access the URL.
	// unit: minutes
	// default: 3
	FailAccessInterval time.Duration `yaml:"failAccessInterval"`

	// gc related
	// GCInitialDelay is the delay time from the start to the first GC execution.
	// default: 6s
	GCInitialDelay time.Duration `yaml:"gcInitialDelay"`

	// GCMetaInterval is the interval time to execute GC meta.
	// default: 2min
	GCMetaInterval time.Duration `yaml:"gcMetaInterval"`

	// GCStorageInterval is the interval time to execute GC storage.
	// default: 15s
	GCStorageInterval time.Duration `yaml:"gcStorageInterval"`

	// TaskExpireTime when a task is not accessed within the taskExpireTime,
	// and it will be treated to be expired.
	// default: 3min
	TaskExpireTime time.Duration `yaml:"taskExpireTime"`

	// disk/hybrid/memory
	StoragePattern string `yaml:"storagePattern"`

	// Console shows log on console
	Console bool `yaml:"console"`
}
