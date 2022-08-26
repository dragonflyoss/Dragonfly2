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
	"errors"
	"time"

	"d7y.io/dragonfly/v2/cmd/dependency/base"
	"d7y.io/dragonfly/v2/pkg/net/fqdn"
	"d7y.io/dragonfly/v2/pkg/net/ip"
	"d7y.io/dragonfly/v2/scheduler/storage"
)

type Config struct {
	// Base options.
	base.Options `yaml:",inline" mapstructure:",squash"`

	// Scheduler configuration.
	Scheduler *SchedulerConfig `yaml:"scheduler" mapstructure:"scheduler"`

	// Server configuration.
	Server *ServerConfig `yaml:"server" mapstructure:"server"`

	// Dynconfig configuration.
	DynConfig *DynConfig `yaml:"dynConfig" mapstructure:"dynConfig"`

	// Manager configuration.
	Manager *ManagerConfig `yaml:"manager" mapstructure:"manager"`

	// SeedPeer configuration.
	SeedPeer *SeedPeerConfig `yaml:"seedPeer" mapstructure:"seedPeer"`

	// Host configuration.
	Host *HostConfig `yaml:"host" mapstructure:"host"`

	// Job configuration.
	Job *JobConfig `yaml:"job" mapstructure:"job"`

	// Storage configuration.
	Storage *StorageConfig `yaml:"storage" mapstructure:"storage"`

	// Metrics configuration.
	Metrics *MetricsConfig `yaml:"metrics" mapstructure:"metrics"`
}

// New default configuration.
func New() *Config {
	return &Config{
		Server: &ServerConfig{
			IP:     ip.IPv4,
			Host:   fqdn.FQDNHostname,
			Listen: DefaultServerListen,
			Port:   DefaultServerPort,
		},
		Scheduler: &SchedulerConfig{
			Algorithm:            DefaultSchedulerAlgorithm,
			BackSourceCount:      DefaultSchedulerBackSourceCount,
			RetryBackSourceLimit: DefaultSchedulerRetryBackSourceLimit,
			RetryLimit:           DefaultSchedulerRetryLimit,
			RetryInterval:        DefaultSchedulerRetryInterval,
			GC: &GCConfig{
				PeerGCInterval: DefaultSchedulerPeerGCInterval,
				PeerTTL:        DefaultSchedulerPeerTTL,
				TaskGCInterval: DefaultSchedulerTaskGCInterval,
				TaskTTL:        DefaultSchedulerTaskTTL,
				HostGCInterval: DefaultSchedulerHostGCInterval,
				HostTTL:        DefaultSchedulerHostTTL,
			},
			Training: &TrainingConfig{
				Enable:               false,
				EnableAutoRefresh:    false,
				RefreshModelInterval: DefaultRefreshModelInterval,
				CPU:                  DefaultCPU,
			},
		},
		DynConfig: &DynConfig{
			RefreshInterval: DefaultDynConfigRefreshInterval,
		},
		Host: &HostConfig{},
		Manager: &ManagerConfig{
			SchedulerClusterID: DefaultManagerSchedulerClusterID,
			KeepAlive: KeepAliveConfig{
				Interval: DefaultManagerKeepAliveInterval,
			},
		},
		SeedPeer: &SeedPeerConfig{
			Enable: true,
		},
		Job: &JobConfig{
			Enable:             true,
			GlobalWorkerNum:    DefaultJobGlobalWorkerNum,
			SchedulerWorkerNum: DefaultJobSchedulerWorkerNum,
			LocalWorkerNum:     DefaultJobLocalWorkerNum,
			Redis: &RedisConfig{
				Port:      DefaultJobRedisPort,
				BrokerDB:  DefaultJobRedisBrokerDB,
				BackendDB: DefaultJobRedisBackendDB,
			},
		},
		Storage: &StorageConfig{
			MaxSize:    storage.DefaultMaxSize,
			MaxBackups: storage.DefaultMaxBackups,
			BufferSize: storage.DefaultBufferSize,
		},
		Metrics: &MetricsConfig{
			Enable:         false,
			Addr:           DefaultMetricsAddr,
			EnablePeerHost: false,
		},
	}
}

// Validate config parameters.
func (cfg *Config) Validate() error {
	if cfg.Server == nil {
		return errors.New("server requires parameter server")
	}

	if cfg.Server.IP == "" {
		return errors.New("server requires parameter ip")
	}

	if cfg.Server.Host == "" {
		return errors.New("server requires parameter host")
	}

	if cfg.Server.Port <= 0 {
		return errors.New("server requires parameter port")
	}

	if cfg.Server.Listen == "" {
		return errors.New("server requires parameter listen")
	}

	if cfg.Scheduler.Algorithm == "" {
		return errors.New("scheduler requires parameter algorithm")
	}

	if cfg.Scheduler.RetryLimit <= 0 {
		return errors.New("scheduler requires parameter retryLimit")
	}

	if cfg.Scheduler.RetryInterval <= 0 {
		return errors.New("scheduler requires parameter retryInterval")
	}

	if cfg.Scheduler.GC == nil {
		return errors.New("scheduler requires parameter gc")
	}

	if cfg.Scheduler.GC.PeerGCInterval <= 0 {
		return errors.New("scheduler requires parameter peerGCInterval")
	}

	if cfg.Scheduler.GC.PeerTTL <= 0 {
		return errors.New("scheduler requires parameter peerTTL")
	}

	if cfg.Scheduler.GC.TaskGCInterval <= 0 {
		return errors.New("scheduler requires parameter taskGCInterval")
	}

	if cfg.Scheduler.GC.TaskTTL <= 0 {
		return errors.New("scheduler requires parameter taskTTL")
	}

	if cfg.Scheduler.Training != nil && cfg.Scheduler.Training.Enable {
		if cfg.Scheduler.Training.CPU <= 0 {
			return errors.New("training requires parameter cpu")
		}

		if cfg.Scheduler.Training.EnableAutoRefresh && cfg.Scheduler.Training.RefreshModelInterval <= 0 {
			return errors.New("training requires parameter refreshModelInterval")
		}
	}

	if cfg.DynConfig.RefreshInterval <= 0 {
		return errors.New("dynconfig requires parameter refreshInterval")
	}

	if cfg.Manager.Addr == "" {
		return errors.New("manager requires parameter addr")
	}

	if cfg.Manager.SchedulerClusterID == 0 {
		return errors.New("manager requires parameter schedulerClusterID")
	}

	if cfg.Manager.KeepAlive.Interval <= 0 {
		return errors.New("manager requires parameter keepAlive interval")
	}

	if cfg.Job != nil && cfg.Job.Enable {
		if cfg.Job.GlobalWorkerNum == 0 {
			return errors.New("job requires parameter globalWorkerNum")
		}

		if cfg.Job.SchedulerWorkerNum == 0 {
			return errors.New("job requires parameter schedulerWorkerNum")
		}

		if cfg.Job.LocalWorkerNum == 0 {
			return errors.New("job requires parameter localWorkerNum")
		}

		if cfg.Job.Redis.Host == "" {
			return errors.New("job requires parameter redis host")
		}

		if cfg.Job.Redis.Port <= 0 {
			return errors.New("job requires parameter redis port")
		}

		if cfg.Job.Redis.BrokerDB <= 0 {
			return errors.New("job requires parameter redis brokerDB")
		}

		if cfg.Job.Redis.BackendDB <= 0 {
			return errors.New("job requires parameter redis backendDB")
		}
	}

	if cfg.Storage == nil {
		return errors.New("server requires parameter storage")
	}

	if cfg.Storage.MaxSize <= 0 {
		return errors.New("storage requires parameter maxSize")
	}

	if cfg.Storage.MaxBackups <= 0 {
		return errors.New("storage requires parameter maxBackups")
	}

	if cfg.Storage.BufferSize <= 0 {
		return errors.New("storage requires parameter bufferSize")
	}

	if cfg.Metrics != nil && cfg.Metrics.Enable {
		if cfg.Metrics.Addr == "" {
			return errors.New("metrics requires parameter addr")
		}
	}

	return nil
}

type ServerConfig struct {
	// Server ip.
	IP string `yaml:"ip" mapstructure:"ip"`

	// Server hostname.
	Host string `yaml:"host" mapstructure:"host"`

	// Listen stands listen interface, like: 0.0.0.0, 192.168.0.1.
	Listen string `yaml:"listen" mapstructure:"listen"`

	// Server port.
	Port int `yaml:"port" mapstructure:"port"`

	// Server work directory.
	WorkHome string `yaml:"workHome" mapstructure:"workHome"`

	// Server dynamic config cache directory.
	CacheDir string `yaml:"cacheDir" mapstructure:"cacheDir"`

	// Server log directory.
	LogDir string `yaml:"logDir" mapstructure:"logDir"`

	// Server storage data directory.
	DataDir string `yaml:"dataDir" mapstructure:"dataDir"`
}

type SchedulerConfig struct {
	// Scheduling algorithm used by the scheduler.
	Algorithm string `yaml:"algorithm" mapstructure:"algorithm"`

	// Single task allows the client to back-to-source count.
	BackSourceCount int `yaml:"backSourceCount" mapstructure:"backSourceCount"`

	// Retry scheduling back-to-source limit times.
	RetryBackSourceLimit int `yaml:"retryBackSourceLimit" mapstructure:"retryBackSourceLimit"`

	// Retry scheduling limit times.
	RetryLimit int `yaml:"retryLimit" mapstructure:"retryLimit"`

	// Retry scheduling interval.
	RetryInterval time.Duration `yaml:"retryInterval" mapstructure:"retryInterval"`

	// Task and peer gc configuration.
	GC *GCConfig `yaml:"gc" mapstructure:"gc"`

	// Training configuration.
	Training *TrainingConfig `yaml:"training" mapstructure:"training"`
}

type TrainingConfig struct {
	// Enable training.
	Enable bool `yaml:"enable" mapstructure:"enable"`

	// Enable auto refresh model.
	EnableAutoRefresh bool `yaml:"enableAutoRefresh" mapstructure:"enableAutoRefresh"`

	// RefreshModelInterval is refresh interval for refreshing model.
	RefreshModelInterval time.Duration `yaml:"refreshModelInterval" mapstructure:"refreshModelInterval"`

	// CPU limit while training.
	CPU int `yaml:"cpu" mapstructure:"cpu"`
}

type GCConfig struct {
	// Peer gc interval.
	PeerGCInterval time.Duration `yaml:"peerGCInterval" mapstructure:"peerGCInterval"`

	// Peer time to live.
	PeerTTL time.Duration `yaml:"peerTTL" mapstructure:"peerTTL"`

	// Task gc interval.
	TaskGCInterval time.Duration `yaml:"taskGCInterval" mapstructure:"taskGCInterval"`

	// Task time to live.
	TaskTTL time.Duration `yaml:"taskTTL" mapstructure:"taskTTL"`

	// Host gc interval.
	HostGCInterval time.Duration `yaml:"hostGCInterval" mapstructure:"hostGCInterval"`

	// Host time to live.
	HostTTL time.Duration `yaml:"hostTTL" mapstructure:"hostTTL"`
}

type DynConfig struct {
	// RefreshInterval is refresh interval for manager cache.
	RefreshInterval time.Duration `yaml:"refreshInterval" mapstructure:"refreshInterval"`
}

type HostConfig struct {
	// IDC for scheduler.
	IDC string `mapstructure:"idc" yaml:"idc"`

	// NetTopology for scheduler.
	NetTopology string `mapstructure:"netTopology" yaml:"netTopology"`

	// Location for scheduler.
	Location string `mapstructure:"location" yaml:"location"`
}

type ManagerConfig struct {
	// Addr is manager address.
	Addr string `yaml:"addr" mapstructure:"addr"`

	// SchedulerClusterID is scheduler cluster id.
	SchedulerClusterID uint `yaml:"schedulerClusterID" mapstructure:"schedulerClusterID"`

	// KeepAlive configuration.
	KeepAlive KeepAliveConfig `yaml:"keepAlive" mapstructure:"keepAlive"`
}

type SeedPeerConfig struct {
	// Enable is to enable seed peer as P2P peer.
	Enable bool `yaml:"enable" mapstructure:"enable"`
}

type KeepAliveConfig struct {
	// Keep alive interval.
	Interval time.Duration `yaml:"interval" mapstructure:"interval"`
}

type JobConfig struct {
	// Enable job service.
	Enable bool `yaml:"enable" mapstructure:"enable"`

	// Number of workers in global queue.
	GlobalWorkerNum uint `yaml:"globalWorkerNum" mapstructure:"globalWorkerNum"`

	// Number of workers in scheduler queue.
	SchedulerWorkerNum uint `yaml:"schedulerWorkerNum" mapstructure:"schedulerWorkerNum"`

	// Number of workers in local queue.
	LocalWorkerNum uint `yaml:"localWorkerNum" mapstructure:"localWorkerNum"`

	// Redis configuration.
	Redis *RedisConfig `yaml:"redis" mapstructure:"redis"`
}

type StorageConfig struct {
	// MaxSize sets the maximum size in megabytes of storage file.
	MaxSize int `yaml:"maxSize" mapstructure:"maxSize"`

	// MaxBackups sets the maximum number of storage files to retain.
	MaxBackups int `yaml:"maxBackups" mapstructure:"maxBackups"`

	// BufferSize sets the size of buffer container,
	// if the buffer is full, write all the records in the buffer to the file.
	BufferSize int `yaml:"bufferSize" mapstructure:"bufferSize"`
}

type RedisConfig struct {
	// Server hostname.
	Host string `yaml:"host" mapstructure:"host"`

	// Server port.
	Port int `yaml:"port" mapstructure:"port"`

	// Server password.
	Password string `yaml:"password" mapstructure:"password"`

	// Broker database name.
	BrokerDB int `yaml:"brokerDB" mapstructure:"brokerDB"`

	// Backend database name.
	BackendDB int `yaml:"backendDB" mapstructure:"backendDB"`
}

type MetricsConfig struct {
	// Enable metrics service.
	Enable bool `yaml:"enable" mapstructure:"enable"`

	// Metrics service address.
	Addr string `yaml:"addr" mapstructure:"addr"`

	// Enable peer host metrics.
	EnablePeerHost bool `yaml:"enablePeerHost" mapstructure:"enablePeerHost"`
}
