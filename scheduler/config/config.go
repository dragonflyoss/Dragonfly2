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

	"github.com/pkg/errors"

	"d7y.io/dragonfly/v2/cmd/dependency/base"
	"d7y.io/dragonfly/v2/pkg/util/hostutils"
	"d7y.io/dragonfly/v2/pkg/util/net/iputils"
)

type Config struct {
	// Base options
	base.Options `yaml:",inline" mapstructure:",squash"`

	// Scheduler configuration
	Scheduler *SchedulerConfig `yaml:"scheduler" mapstructure:"scheduler"`

	// Server configuration
	Server *ServerConfig `yaml:"server" mapstructure:"server"`

	// Dynconfig configuration
	DynConfig *DynConfig `yaml:"dynConfig" mapstructure:"dynConfig"`

	// Manager configuration
	Manager *ManagerConfig `yaml:"manager" mapstructure:"manager"`

	// Host configuration
	Host *HostConfig `yaml:"host" mapstructure:"host"`

	// Job configuration
	Job *JobConfig `yaml:"job" mapstructure:"job"`

	// Metrics configuration
	Metrics *MetricsConfig `yaml:"metrics" mapstructure:"metrics"`
}

// New default configuration
func New() *Config {
	return &Config{
		Server: &ServerConfig{
			IP:          iputils.IPv4,
			Host:        hostutils.FQDNHostname,
			Port:        8002,
			ListenLimit: 1000,
		},
		Scheduler: &SchedulerConfig{
			Algorithm:       "default",
			BackSourceCount: 3,
			RetryLimit:      10,
			RetryInterval:   1 * time.Second,
			GC: &GCConfig{
				PeerGCInterval: 10 * time.Minute,
				PeerTTL:        24 * time.Hour,
				TaskGCInterval: 10 * time.Minute,
				TaskTTL:        24 * time.Hour,
			},
		},
		DynConfig: &DynConfig{
			RefreshInterval: 1 * time.Minute,
		},
		Host: &HostConfig{},
		Manager: &ManagerConfig{
			Enable:             true,
			SchedulerClusterID: 1,
			KeepAlive: KeepAliveConfig{
				Interval: 5 * time.Second,
			},
		},
		Job: &JobConfig{
			Enable:             true,
			GlobalWorkerNum:    10,
			SchedulerWorkerNum: 10,
			LocalWorkerNum:     10,
			Redis: &RedisConfig{
				Port:      6379,
				BrokerDB:  1,
				BackendDB: 2,
			},
		},
		Metrics: &MetricsConfig{
			Enable:         false,
			EnablePeerHost: false,
		},
	}
}

// Validate config parameters
func (c *Config) Validate() error {
	if c.Server.IP == "" {
		return errors.New("server requires parameter ip")
	}

	if c.Server.Host == "" {
		return errors.New("server requires parameter host")
	}

	if c.Server.Port <= 0 {
		return errors.New("server requires parameter port")
	}

	if c.Server.ListenLimit <= 0 {
		return errors.New("server requires parameter listenLimit")
	}

	if c.Scheduler.Algorithm == "" {
		return errors.New("scheduler requires parameter algorithm")
	}

	if c.Scheduler.RetryLimit <= 0 {
		return errors.New("scheduler requires parameter retryLimit")
	}

	if c.Scheduler.RetryInterval <= 0 {
		return errors.New("scheduler requires parameter retryInterval")
	}

	if c.Scheduler.GC.PeerGCInterval <= 0 {
		return errors.New("scheduler requires parameter peerGCInterval")
	}

	if c.Scheduler.GC.PeerTTL <= 0 {
		return errors.New("scheduler requires parameter peerTTL")
	}

	if c.Scheduler.GC.TaskGCInterval <= 0 {
		return errors.New("scheduler requires parameter taskGCInterval")
	}

	if c.Scheduler.GC.TaskTTL <= 0 {
		return errors.New("scheduler requires parameter taskTTL")
	}

	if c.DynConfig.RefreshInterval <= 0 {
		return errors.New("dynconfig requires parameter refreshInterval")
	}

	if c.Manager.Enable {
		if c.Manager.Addr == "" {
			return errors.New("manager requires parameter addr")
		}

		if c.Manager.SchedulerClusterID == 0 {
			return errors.New("manager requires parameter schedulerClusterID")
		}

		if c.Manager.KeepAlive.Interval <= 0 {
			return errors.New("manager requires parameter keepAlive interval")
		}
	}

	if c.Job.Enable {
		if c.Job.GlobalWorkerNum == 0 {
			return errors.New("job requires parameter globalWorkerNum")
		}

		if c.Job.SchedulerWorkerNum == 0 {
			return errors.New("job requires parameter schedulerWorkerNum")
		}

		if c.Job.LocalWorkerNum == 0 {
			return errors.New("job requires parameter localWorkerNum")
		}

		if c.Job.Redis.Host == "" {
			return errors.New("job requires parameter redis host")
		}

		if c.Job.Redis.Port <= 0 {
			return errors.New("job requires parameter redis port")
		}

		if c.Job.Redis.BrokerDB <= 0 {
			return errors.New("job requires parameter redis brokerDB")
		}

		if c.Job.Redis.BackendDB <= 0 {
			return errors.New("job requires parameter redis backendDB")
		}
	}

	if c.Metrics.Enable {
		if c.Metrics.Addr == "" {
			return errors.New("metrics requires parameter addr")
		}
	}

	return nil
}

type ServerConfig struct {
	// Server ip
	IP string `yaml:"ip" mapstructure:"ip"`

	// Server hostname
	Host string `yaml:"host" mapstructure:"host"`

	// Server port
	Port int `yaml:"port" mapstructure:"port"`

	// Limit the number of requests
	ListenLimit int `yaml:"listenLimit" mapstructure:"listenLimit"`

	// Server dynamic config cache directory
	CacheDir string `yaml:"cacheDir" mapstructure:"cacheDir"`

	// Server log directory
	LogDir string `yaml:"logDir" mapstructure:"logDir"`
}

type SchedulerConfig struct {
	// Scheduling algorithm used by the scheduler
	Algorithm string `yaml:"algorithm" mapstructure:"algorithm"`

	// Single task allows the client to back-to-source count
	BackSourceCount int `yaml:"backSourceCount" mapstructure:"backSourceCount"`

	// Retry scheduling limit times
	RetryLimit int `yaml:"retryLimit" mapstructure:"retryLimit"`

	// Retry scheduling interval
	RetryInterval time.Duration `yaml:"retryInterval" mapstructure:"retryInterval"`

	// Task and peer gc configuration
	GC *GCConfig `yaml:"gc" mapstructure:"gc"`
}

type GCConfig struct {
	// Peer gc interval
	PeerGCInterval time.Duration `yaml:"peerGCInterval" mapstructure:"peerGCInterval"`

	// Peer time to live
	PeerTTL time.Duration `yaml:"peerTTL" mapstructure:"peerTTL"`

	// Task gc interval
	TaskGCInterval time.Duration `yaml:"taskGCInterval" mapstructure:"taskGCInterval"`

	// Task time to live
	TaskTTL time.Duration `yaml:"taskTTL" mapstructure:"taskTTL"`
}

type DynConfig struct {
	// RefreshInterval is refresh interval for manager cache.
	RefreshInterval time.Duration `yaml:"refreshInterval" mapstructure:"refreshInterval"`

	// CDNDir is cdn dir path.
	CDNDir string `yaml:"cdnDir" mapstructure:"cdnDir"`
}

type HostConfig struct {
	// IDC for scheduler
	IDC string `mapstructure:"idc" yaml:"idc"`

	// NetTopology for scheduler
	NetTopology string `mapstructure:"netTopology" yaml:"netTopology"`

	// Location for scheduler
	Location string `mapstructure:"location" yaml:"location"`
}

type ManagerConfig struct {
	// Enable is to enable contact with manager
	Enable bool `yaml:"enable" mapstructure:"enable"`

	// Addr is manager address.
	Addr string `yaml:"addr" mapstructure:"addr"`

	// SchedulerClusterID is scheduler cluster id.
	SchedulerClusterID uint `yaml:"schedulerClusterID" mapstructure:"schedulerClusterID"`

	// KeepAlive configuration
	KeepAlive KeepAliveConfig `yaml:"keepAlive" mapstructure:"keepAlive"`
}

type KeepAliveConfig struct {
	// Keep alive interval
	Interval time.Duration `yaml:"interval" mapstructure:"interval"`
}

type JobConfig struct {
	// Enable job service
	Enable bool `yaml:"enable" mapstructure:"enable"`

	// Number of workers in global queue
	GlobalWorkerNum uint `yaml:"globalWorkerNum" mapstructure:"globalWorkerNum"`

	// Number of workers in scheduler queue
	SchedulerWorkerNum uint `yaml:"schedulerWorkerNum" mapstructure:"schedulerWorkerNum"`

	// Number of workers in local queue
	LocalWorkerNum uint `yaml:"localWorkerNum" mapstructure:"localWorkerNum"`

	// Redis configuration
	Redis *RedisConfig `yaml:"redis" mapstructure:"redis"`
}

type RedisConfig struct {
	// Server hostname
	Host string `yaml:"host" mapstructure:"host"`

	// Server port
	Port int `yaml:"port" mapstructure:"port"`

	// Server password
	Password string `yaml:"password" mapstructure:"password"`

	// Broker database name
	BrokerDB int `yaml:"brokerDB" mapstructure:"brokerDB"`

	// Backend database name
	BackendDB int `yaml:"backendDB" mapstructure:"backendDB"`
}

type MetricsConfig struct {
	// Enable metrics service
	Enable bool `yaml:"enable" mapstructure:"enable"`

	// Metrics service address
	Addr string `yaml:"addr" mapstructure:"addr"`

	// Enable peer host metrics
	EnablePeerHost bool `yaml:"enablePeerHost" mapstructure:"enablePeerHost"`
}
