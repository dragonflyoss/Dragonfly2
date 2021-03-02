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
	"d7y.io/dragonfly/v2/pkg/rate"
	"d7y.io/dragonfly/v2/pkg/util/fileutils"
	"github.com/mitchellh/go-homedir"
	"gopkg.in/yaml.v3"
	"path/filepath"
	"time"
)

// NewConfig creates an instant with default values.
func NewConfig() *Config {
	return &Config{
		BaseProperties: NewBaseProperties(),
		Plugins:        nil,
	}
}

// Config contains all configuration of cdnNode.
type Config struct {
	*BaseProperties `yaml:"base"`
	Plugins         map[PluginType][]*PluginProperties `yaml:"plugins"`
}

// Load loads config properties from the giving file.
func (c *Config) Load(path string) error {
	return fileutils.LoadYaml(path, c)
}

func (c *Config) String() string {
	if out, err := yaml.Marshal(c); err == nil {
		return string(out)
	}
	return ""
}

// NewBaseProperties creates an instant with default values.
func NewBaseProperties() *BaseProperties {
	userHome, err := homedir.Dir()
	var home string
	home = filepath.Join(string(filepath.Separator), userHome, "cdn-system")
	if err != nil {
		home = filepath.Join(string(filepath.Separator), "home", "admin", "cdn-system")
	}

	return &BaseProperties{
		ListenPort:              DefaultListenPort,
		DownloadPort:            DefaultDownloadPort,
		HomeDir:                 home,
		StoragePattern:          DefaultStoragePattern,
		//DownloadPath:            filepath.Join(home, RepoHome, DownloadHome),
		SystemReservedBandwidth: DefaultSystemReservedBandwidth,
		MaxBandwidth:            DefaultMaxBandwidth,
		EnableProfiler:          false,
		FailAccessInterval:      DefaultFailAccessInterval,
		GCInitialDelay:          DefaultGCInitialDelay,
		GCMetaInterval:          DefaultGCMetaInterval,
		GCDiskInterval:          DefaultGCDiskInterval,
		YoungGCThreshold:        DefaultYoungGCThreshold,
		FullGCThreshold:         DefaultFullGCThreshold,
		IntervalThreshold:       DefaultIntervalThreshold,
		TaskExpireTime:          DefaultTaskExpireTime,
		CleanRatio:              DefaultCleanRatio,
	}
}

// BaseProperties contains all basic properties of cdn system.
type BaseProperties struct {
	// disk/hybrid/memory
	StoragePattern string `yaml:"storagePattern"`

	// ListenPort is the port cdn server listens on.
	// default: 8002
	ListenPort int `yaml:"listenPort"`

	// DownloadPort is the port for download files from cdn.
	// default: 8001
	DownloadPort int `yaml:"downloadPort"`

	// HomeDir is working directory of cdn.
	// default: /home/admin/cdn-system
	HomeDir string `yaml:"homeDir"`

	// DownloadPath specifies the path where to store downloaded files from source address.
	//DownloadPath string `yaml:"downloadPath"`

	// SystemReservedBandwidth is the network bandwidth reserved for system software.
	// default: 20 MB, in format of G(B)/g/M(B)/m/K(B)/k/B, pure number will also be parsed as Byte.
	SystemReservedBandwidth rate.Rate `yaml:"systemReservedBandwidth"`

	// MaxBandwidth is the network bandwidth that cdn system can use.
	// default: 200 MB, in format of G(B)/g/M(B)/m/K(B)/k/B, pure number will also be parsed as Byte.
	MaxBandwidth rate.Rate `yaml:"maxBandwidth"`

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

	// TaskExpireTime when a task is not accessed within the taskExpireTime,
	// and it will be treated to be expired.
	// default: 3min
	TaskExpireTime time.Duration `yaml:"taskExpireTime"`

	// GCDiskInterval is the interval time to execute GC disk.
	// default: 15s
	GCDiskInterval time.Duration `yaml:"gcDiskInterval"`

	GCShmInterval time.Duration `yaml:"gcShmInterval"`

	// YoungGCThreshold if the available disk space is more than YoungGCThreshold
	// and there is no need to GC disk.
	//
	// default: 100GB
	YoungGCThreshold fileutils.Fsize `yaml:"youngGCThreshold"`

	// FullGCThreshold if the available disk space is less than FullGCThreshold
	// and the cdn system should gc all task files which are not being used.
	//
	// default: 5GB
	FullGCThreshold fileutils.Fsize `yaml:"fullGCThreshold"`

	// MaxStorageThreshold if the currently used disk space is greater than MaxStorageThreshold, clean disk up
	MaxStorageThreshold fileutils.Fsize

	// IntervalThreshold is the threshold of the interval at which the task file is accessed.
	// default: 2h
	IntervalThreshold time.Duration `yaml:"IntervalThreshold"`

	// CleanRatio is the ratio to clean the disk and it is based on 10.
	// It means the value of CleanRatio should be [1-10].
	//
	// default: 1
	CleanRatio int `yaml:"cleanRatio"`
}
