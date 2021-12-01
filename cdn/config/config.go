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
	"time"

	"gopkg.in/yaml.v3"

	"d7y.io/dragonfly/v2/cdn/plugins"
	"d7y.io/dragonfly/v2/cdn/storedriver"
	"d7y.io/dragonfly/v2/cdn/storedriver/local"
	"d7y.io/dragonfly/v2/cdn/supervisor/cdn/storage"
	"d7y.io/dragonfly/v2/cdn/supervisor/cdn/storage/disk"
	"d7y.io/dragonfly/v2/cdn/supervisor/cdn/storage/hybrid"
	"d7y.io/dragonfly/v2/cmd/dependency/base"
	"d7y.io/dragonfly/v2/pkg/unit"
	"d7y.io/dragonfly/v2/pkg/util/net/iputils"
)

// New creates an instant with default values.
func New() *Config {
	return &Config{
		BaseProperties: NewDefaultBaseProperties(),
		Plugins:        NewDefaultPlugins(),
	}
}

// Config contains all configuration of cdn node.
type Config struct {
	base.Options    `yaml:",inline" mapstructure:",squash"`
	*BaseProperties `yaml:"base" mapstructure:"base"`

	Plugins map[plugins.PluginType][]*plugins.PluginProperties `yaml:"plugins" mapstructure:"plugins"`
}

func (c *Config) String() string {
	if out, err := yaml.Marshal(c); err == nil {
		return string(out)
	}
	return ""
}

// NewDefaultPlugins creates plugin instants with default values.
func NewDefaultPlugins() map[plugins.PluginType][]*plugins.PluginProperties {
	return map[plugins.PluginType][]*plugins.PluginProperties{
		plugins.StorageDriverPlugin: {
			{
				Name:   local.DiskDriverName,
				Enable: true,
				Config: &storedriver.Config{
					BaseDir: DefaultDiskBaseDir,
				},
			}, {
				Name:   local.MemoryDriverName,
				Enable: false,
				Config: &storedriver.Config{
					BaseDir: DefaultMemoryBaseDir,
				},
			},
		}, plugins.StorageManagerPlugin: {
			{
				Name:   disk.StorageMode,
				Enable: true,
				Config: &storage.Config{
					GCInitialDelay: 0 * time.Second,
					GCInterval:     15 * time.Second,
					DriverConfigs: map[string]*storage.DriverConfig{
						local.DiskDriverName: {
							GCConfig: &storage.GCConfig{
								YoungGCThreshold:  100 * unit.GB,
								FullGCThreshold:   5 * unit.GB,
								CleanRatio:        1,
								IntervalThreshold: 2 * time.Hour,
							}},
					},
				},
			}, {
				Name:   hybrid.StorageMode,
				Enable: false,
				Config: &storage.Config{
					GCInitialDelay: 0 * time.Second,
					GCInterval:     15 * time.Second,
					DriverConfigs: map[string]*storage.DriverConfig{
						local.DiskDriverName: {
							GCConfig: &storage.GCConfig{
								YoungGCThreshold:  100 * unit.GB,
								FullGCThreshold:   5 * unit.GB,
								CleanRatio:        1,
								IntervalThreshold: 2 * time.Hour,
							},
						},
						local.MemoryDriverName: {
							GCConfig: &storage.GCConfig{
								YoungGCThreshold:  100 * unit.GB,
								FullGCThreshold:   5 * unit.GB,
								CleanRatio:        3,
								IntervalThreshold: 2 * time.Hour,
							},
						},
					},
				},
			},
		},
	}
}

// NewDefaultBaseProperties creates an base properties instant with default values.
func NewDefaultBaseProperties() *BaseProperties {
	return &BaseProperties{
		ListenPort:              DefaultListenPort,
		DownloadPort:            DefaultDownloadPort,
		SystemReservedBandwidth: DefaultSystemReservedBandwidth,
		MaxBandwidth:            DefaultMaxBandwidth,
		FailAccessInterval:      DefaultFailAccessInterval,
		GCInitialDelay:          DefaultGCInitialDelay,
		GCMetaInterval:          DefaultGCMetaInterval,
		TaskExpireTime:          DefaultTaskExpireTime,
		StorageMode:             DefaultStorageMode,
		AdvertiseIP:             iputils.IPv4,
		Manager: ManagerConfig{
			KeepAlive: KeepAliveConfig{
				Interval: DefaultKeepAliveInterval,
			},
		},
		Host: HostConfig{},
	}
}

// BaseProperties contains all basic properties of cdn system.
type BaseProperties struct {
	// ListenPort is the port cdn server listens on.
	// default: 8002
	ListenPort int `yaml:"listenPort" mapstructure:"listenPort"`

	// DownloadPort is the port for download files from cdn.
	// default: 8001
	DownloadPort int `yaml:"downloadPort" mapstructure:"downloadPort"`

	// SystemReservedBandwidth is the network bandwidth reserved for system software.
	// default: 20 MB, in format of G(B)/g/M(B)/m/K(B)/k/B, pure number will also be parsed as Byte.
	SystemReservedBandwidth unit.Bytes `yaml:"systemReservedBandwidth" mapstructure:"systemReservedBandwidth"`

	// MaxBandwidth is the network bandwidth that cdn system can use.
	// default: 200 MB, in format of G(B)/g/M(B)/m/K(B)/k/B, pure number will also be parsed as Byte.
	MaxBandwidth unit.Bytes `yaml:"maxBandwidth" mapstructure:"maxBandwidth"`

	// AdvertiseIP is used to set the ip that we advertise to other peer in the p2p-network.
	// By default, the first non-loop address is advertised.
	AdvertiseIP string `yaml:"advertiseIP" mapstructure:"advertiseIP"`

	// FailAccessInterval is the interval time after failed to access the URL.
	// unit: minutes
	// default: 3
	FailAccessInterval time.Duration `yaml:"failAccessInterval" mapstructure:"failAccessInterval"`

	// gc related
	// GCInitialDelay is the delay time from the start to the first GC execution.
	// default: 6s
	GCInitialDelay time.Duration `yaml:"gcInitialDelay" mapstructure:"gcInitialDelay"`

	// GCMetaInterval is the interval time to execute GC meta.
	// default: 2min
	GCMetaInterval time.Duration `yaml:"gcMetaInterval" mapstructure:"gcMetaInterval"`

	// TaskExpireTime when a task is not accessed within the taskExpireTime,
	// and it will be treated to be expired.
	// default: 3min
	TaskExpireTime time.Duration `yaml:"taskExpireTime" mapstructure:"taskExpireTime"`

	// StorageMode disk/hybrid/memory
	StorageMode string `yaml:"storageMode" mapstructure:"storageMode"`

	// Manager configuration
	Manager ManagerConfig `yaml:"manager" mapstructure:"manager"`

	// Host configuration
	Host HostConfig `yaml:"host" mapstructure:"host"`

	// Metrics configuration
	Metrics *RestConfig `yaml:"metrics" mapstructure:"metrics"`
}

type ManagerConfig struct {
	// NetAddr is manager address.
	Addr string `yaml:"addr" mapstructure:"addr"`

	// CDNClusterID is cdn cluster id.
	CDNClusterID uint `yaml:"cdnClusterID" mapstructure:"cdnClusterID"`

	// KeepAlive configuration
	KeepAlive KeepAliveConfig `yaml:"keepAlive" mapstructure:"keepAlive"`
}

type KeepAliveConfig struct {
	// Keep alive interval
	Interval time.Duration `yaml:"interval" mapstructure:"interval"`
}

type HostConfig struct {
	// Location for scheduler
	Location string `mapstructure:"location" yaml:"location"`

	// IDC for scheduler
	IDC string `mapstructure:"idc" yaml:"idc"`
}

type RestConfig struct {
	Addr string `yaml:"addr" mapstructure:"addr"`
}
