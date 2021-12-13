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
	"path/filepath"
	"time"

	"d7y.io/dragonfly/v2/cdn/metrics"
	"d7y.io/dragonfly/v2/cdn/plugins"
	"d7y.io/dragonfly/v2/cdn/rpcserver"
	"d7y.io/dragonfly/v2/cdn/storedriver"
	"d7y.io/dragonfly/v2/cdn/supervisor/cdn"
	"d7y.io/dragonfly/v2/cdn/supervisor/task"
	"d7y.io/dragonfly/v2/cmd/dependency/base"
	"d7y.io/dragonfly/v2/pkg/basic"
	"d7y.io/dragonfly/v2/pkg/unit"
	"d7y.io/dragonfly/v2/pkg/util/net/iputils"
)

// NewDeprecatedConfig creates an instant with default values.
func NewDeprecatedConfig() *DeprecatedConfig {
	return &DeprecatedConfig{
		BaseProperties: NewDefaultBaseProperties(),
		Plugins:        NewDefaultPlugins(),
	}
}

// DeprecatedConfig contains all configuration of cdn node.
type DeprecatedConfig struct {
	base.Options    `yaml:",inline" mapstructure:",squash"`
	*BaseProperties `yaml:"base" mapstructure:"base"`

	Plugins map[plugins.PluginType][]*plugins.PluginProperties `yaml:"plugins" mapstructure:"plugins"`
}

func (c DeprecatedConfig) Convert() *Config {
	newConfig := New()
	base := c.BaseProperties
	newConfig.Manager = base.Manager
	newConfig.Host = base.Host
	newConfig.LogDir = base.LogDir
	if base.Metrics != nil {
		newConfig.Metrics = metrics.Config{
			Net:  "tcp",
			Addr: base.Metrics.Addr,
		}
	}
	newConfig.Task = task.Config{
		GCInitialDelay:     base.GCInitialDelay,
		GCMetaInterval:     base.GCMetaInterval,
		ExpireTime:         base.TaskExpireTime,
		FailAccessInterval: base.FailAccessInterval,
	}
	newConfig.CDN = cdn.Config{
		SystemReservedBandwidth: base.SystemReservedBandwidth,
		MaxBandwidth:            base.MaxBandwidth,
	}
	newConfig.RPCServer = rpcserver.Config{
		AdvertiseIP:  base.AdvertiseIP,
		ListenPort:   base.ListenPort,
		DownloadPort: base.DownloadPort,
	}
	newConfig.Verbose = c.Verbose
	newConfig.Console = c.Console
	newConfig.Telemetry = c.Telemetry
	return newConfig
}

// NewDefaultPlugins creates plugin instants with default values.
func NewDefaultPlugins() map[plugins.PluginType][]*plugins.PluginProperties {
	return map[plugins.PluginType][]*plugins.PluginProperties{
		plugins.StorageDriverPlugin: {
			{
				Name:   "disk",
				Enable: true,
				Config: &storedriver.Config{
					BaseDir: filepath.Join(basic.HomeDir, "ftp"),
				},
			}, {
				Name:   "memory",
				Enable: false,
				Config: &storedriver.Config{
					BaseDir: "/dev/shm/dragonfly",
				},
			},
		},
	}
}

// NewDefaultBaseProperties creates an base properties instant with default values.
func NewDefaultBaseProperties() *BaseProperties {
	return &BaseProperties{
		ListenPort:              8003,
		DownloadPort:            8001,
		SystemReservedBandwidth: 20 * unit.MB,
		MaxBandwidth:            1 * unit.GB,
		AdvertiseIP:             iputils.IPv4,
		FailAccessInterval:      3 * time.Minute,
		GCInitialDelay:          6 * time.Second,
		GCMetaInterval:          2 * time.Minute,
		TaskExpireTime:          3 * time.Minute,
		StorageMode:             "disk",
		Manager: ManagerConfig{
			KeepAlive: KeepAliveConfig{
				Interval: 5 * time.Second,
			},
		},
		Host: HostConfig{},
		Metrics: &RestConfig{
			Addr: ":8080",
		},
	}
}

type StorageConfig struct {
	GCInitialDelay time.Duration            `yaml:"gcInitialDelay"`
	GCInterval     time.Duration            `yaml:"gcInterval"`
	DriverConfigs  map[string]*DriverConfig `yaml:"driverConfigs"`
}

type DriverConfig struct {
	GCConfig *GCConfig `yaml:"gcConfig"`
}

// GCConfig gc config
type GCConfig struct {
	YoungGCThreshold  unit.Bytes    `yaml:"youngGCThreshold"`
	FullGCThreshold   unit.Bytes    `yaml:"fullGCThreshold"`
	CleanRatio        int           `yaml:"cleanRatio"`
	IntervalThreshold time.Duration `yaml:"intervalThreshold"`
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

	// Log directory
	LogDir string `yaml:"logDir" mapstructure:"logDir"`

	// Manager configuration
	Manager ManagerConfig `yaml:"manager" mapstructure:"manager"`

	// Host configuration
	Host HostConfig `yaml:"host" mapstructure:"host"`

	// Metrics configuration
	Metrics *RestConfig `yaml:"metrics" mapstructure:"metrics"`
}

type RestConfig struct {
	Addr string `yaml:"addr" mapstructure:"addr"`
}
