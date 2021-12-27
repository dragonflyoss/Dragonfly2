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
	"net"
	"time"

	"github.com/pkg/errors"

	"d7y.io/dragonfly/v2/cmd/dependency/base"
	"d7y.io/dragonfly/v2/pkg/util/hostutils"
	"d7y.io/dragonfly/v2/pkg/util/net/iputils"
)

type Config struct {
	base.Options `yaml:",inline" mapstructure:",squash"`
	Scheduler    *SchedulerConfig `yaml:"scheduler" mapstructure:"scheduler"`
	Server       *ServerConfig    `yaml:"server" mapstructure:"server"`
	DynConfig    *DynConfig       `yaml:"dynConfig" mapstructure:"dynConfig"`
	Manager      *ManagerConfig   `yaml:"manager" mapstructure:"manager"`
	Host         *HostConfig      `yaml:"host" mapstructure:"host"`
	Job          *JobConfig       `yaml:"job" mapstructure:"job"`
	Metrics      *MetricsConfig   `yaml:"metrics" mapstructure:"metrics"`
}

func New() *Config {
	return &Config{
		Scheduler: &SchedulerConfig{
			Algorithm:       "default",
			WorkerNum:       10,
			BackSourceCount: 3,
			GC: &GCConfig{
				PeerGCInterval: 1 * time.Minute,
				PeerTTL:        5 * time.Minute,
				TaskGCInterval: 1 * time.Minute,
				TaskTTL:        10 * time.Minute,
			},
		},
		Server: &ServerConfig{
			IP:   iputils.IPv4,
			Host: hostutils.FQDNHostname,
			Port: 8002,
		},
		DynConfig: &DynConfig{
			RefreshInterval: 5 * time.Minute,
		},
		Manager: &ManagerConfig{
			Addr:               "",
			SchedulerClusterID: 1,
			KeepAlive: KeepAliveConfig{
				Interval: 5 * time.Second,
			},
		},
		Host: &HostConfig{
			IDC:         "",
			NetTopology: "",
			Location:    "",
		},
		Job: &JobConfig{
			GlobalWorkerNum:    10,
			SchedulerWorkerNum: 10,
			LocalWorkerNum:     10,
			Redis: &RedisConfig{
				Host:      "",
				Port:      6379,
				Password:  "",
				BrokerDB:  1,
				BackendDB: 2,
			},
		},
	}
}

func (c *Config) Validate() error {
	if c.DynConfig.RefreshInterval == 0 {
		return errors.New("dynconfig is ManagerSourceType type requires parameter refreshInterval")
	}

	if c.DynConfig.CDNDir == "" && c.Manager.Addr == "" {
		return errors.New("dynconfig is ManagerSourceType type requires parameter manager addr")
	}

	if c.Manager.SchedulerClusterID == 0 {
		return errors.New("dynconfig is ManagerSourceType type requires parameter manager schedulerClusterID")
	}

	return nil
}

func (c *Config) Convert() error {
	if c.Manager.Addr != "" && c.Job.Redis.Host == "" {
		host, _, err := net.SplitHostPort(c.Manager.Addr)
		if err != nil {
			return err
		}
		c.Job.Redis.Host = host
	}
	return nil
}

type ManagerConfig struct {
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

type DynConfig struct {
	// RefreshInterval is refresh interval for manager cache.
	RefreshInterval time.Duration `yaml:"refreshInterval" mapstructure:"refreshInterval"`

	// CDNDir is cdn dir path.
	CDNDir string `yaml:"cdnDir" mapstructure:"cdnDir"`
}

type SchedulerConfig struct {
	Algorithm       string    `yaml:"algorithm" mapstructure:"algorithm"`
	WorkerNum       int       `yaml:"workerNum" mapstructure:"workerNum"`
	BackSourceCount int32     `yaml:"backSourceCount" mapstructure:"backSourceCount"`
	GC              *GCConfig `yaml:"gc" mapstructure:"gc"`
}

type ServerConfig struct {
	IP       string `yaml:"ip" mapstructure:"ip"`
	Host     string `yaml:"host" mapstructure:"host"`
	Port     int    `yaml:"port" mapstructure:"port"`
	CacheDir string `yaml:"cacheDir" mapstructure:"cacheDir"`
	LogDir   string `yaml:"logDir" mapstructure:"logDir"`
}

type GCConfig struct {
	PeerGCInterval time.Duration `yaml:"peerGCInterval" mapstructure:"peerGCInterval"`
	PeerTTL        time.Duration `yaml:"peerTTL" mapstructure:"peerTTL"`
	TaskGCInterval time.Duration `yaml:"taskGCInterval" mapstructure:"taskGCInterval"`
	TaskTTL        time.Duration `yaml:"taskTTL" mapstructure:"taskTTL"`
}

type MetricsConfig struct {
	Addr           string `yaml:"addr" mapstructure:"addr"`
	EnablePeerHost bool   `yaml:"enablePeerHost" mapstructure:"enablePeerHost"`
}

type HostConfig struct {
	// IDC for scheduler
	IDC string `mapstructure:"idc" yaml:"idc"`

	// NetTopology for scheduler
	NetTopology string `mapstructure:"netTopology" yaml:"netTopology"`

	// Location for scheduler
	Location string `mapstructure:"location" yaml:"location"`
}

type RedisConfig struct {
	Host      string `yaml:"host" mapstructure:"host"`
	Port      int    `yaml:"port" mapstructure:"port"`
	Password  string `yaml:"password" mapstructure:"password"`
	BrokerDB  int    `yaml:"brokerDB" mapstructure:"brokerDB"`
	BackendDB int    `yaml:"backendDB" mapstructure:"backendDB"`
}

type JobConfig struct {
	GlobalWorkerNum    uint         `yaml:"globalWorkerNum" mapstructure:"globalWorkerNum"`
	SchedulerWorkerNum uint         `yaml:"schedulerWorkerNum" mapstructure:"schedulerWorkerNum"`
	LocalWorkerNum     uint         `yaml:"localWorkerNum" mapstructure:"localWorkerNum"`
	Redis              *RedisConfig `yaml:"redis" mapstructure:"redis"`
}
