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
	"runtime"
	"strings"
	"time"

	"d7y.io/dragonfly/v2/cmd/dependency/base"
	dc "d7y.io/dragonfly/v2/internal/dynconfig"
	"d7y.io/dragonfly/v2/pkg/util/net/iputils"
	"github.com/pkg/errors"
)

type Config struct {
	base.Options `yaml:",inline" mapstructure:",squash"`
	Scheduler    *SchedulerConfig `yaml:"scheduler" mapstructure:"scheduler"`
	Server       *ServerConfig    `yaml:"server" mapstructure:"server"`
	DynConfig    *DynConfig       `yaml:"dynConfig" mapstructure:"dynConfig"`
	Manager      *ManagerConfig   `yaml:"manager" mapstructure:"manager"`
	Host         *HostConfig      `yaml:"host" mapstructure:"host"`
	Task         *TaskConfig      `yaml:"task" mapstructure:"task"`
}

func New() *Config {
	return (&Config{
		Scheduler: NewDefaultSchedulerConfig(),
		Server:    NewDefaultServerConfig(),
		DynConfig: NewDefaultDynConfig(),
		Manager:   NewDefaultManagerConfig(),
		Host:      NewHostConfig(),
		Task:      NewDefaultTaskConfig(),
	}).ConvertRedisHost()
}

func NewHostConfig() *HostConfig {
	return &HostConfig{
		Location: "",
		IDC:      "",
	}
}

func (c *Config) Validate() error {
	if c.DynConfig.CDNDirPath == "" {
		if c.DynConfig.Type == dc.LocalSourceType && c.DynConfig.Data == nil {
			return errors.New("dynconfig is LocalSourceType type requires parameter data")
		}
	}

	if c.DynConfig.Type == dc.ManagerSourceType {
		if c.DynConfig.ExpireTime == 0 {
			return errors.New("dynconfig is ManagerSourceType type requires parameter expireTime")
		}

		if c.Manager.Addr == "" {
			return errors.New("dynconfig is ManagerSourceType type requires parameter manager addr")
		}
	}

	return nil
}

func NewDefaultDynConfig() *DynConfig {
	return &DynConfig{
		Type:       dc.LocalSourceType,
		ExpireTime: 60000 * 1000 * 1000,
		CDNDirPath: "",
		Data: &DynconfigData{
			CDNs: []*CDN{
				{
					HostName:      "localhost",
					IP:            "127.0.0.1",
					Port:          8003,
					DownloadPort:  8001,
					SecurityGroup: "",
					Location:      "",
					IDC:           "",
					NetTopology:   "",
				},
			},
		},
	}
}

func NewDefaultServerConfig() *ServerConfig {
	return &ServerConfig{
		IP:   iputils.HostIP,
		Port: 8002,
	}
}

func NewDefaultSchedulerConfig() *SchedulerConfig {
	return &SchedulerConfig{
		DisableCDN:           false,
		ABTest:               false,
		AScheduler:           "",
		BScheduler:           "",
		WorkerNum:            runtime.GOMAXPROCS(0),
		Monitor:              false,
		AccessWindow:         3 * time.Minute,
		CandidateParentCount: 10,
		Scheduler:            "basic",
		CDNLoad:              100,
		ClientLoad:           10,
		OpenMonitor:          false,
		GC:                   NewDefaultGCConfig(),
	}
}

func NewDefaultGCConfig() *GCConfig {
	return &GCConfig{
		PeerGCInterval: 5 * time.Minute,
		TaskGCInterval: 5 * time.Minute,
		PeerTTL:        5 * time.Minute,
		TaskTTL:        1 * time.Hour,
	}
}

func NewDefaultManagerConfig() *ManagerConfig {
	return &ManagerConfig{
		Addr:               "",
		SchedulerClusterID: 0,
		KeepAlive: KeepAliveConfig{
			Interval:         5 * time.Second,
			RetryMaxAttempts: 100000000,
			RetryInitBackOff: 5,
			RetryMaxBackOff:  10,
		},
	}
}

func NewDefaultTaskConfig() *TaskConfig {
	return &TaskConfig{
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
	}
}

func (c *Config) ConvertRedisHost() *Config {
	if c.Manager != nil && c.Task != nil && c.Task.Redis != nil {
		n := strings.LastIndex(c.Manager.Addr, ":")
		if n >= 0 {
			if ip := net.ParseIP(c.Manager.Addr[0:n]); ip != nil && !net.IPv4zero.Equal(ip) {
				c.Task.Redis.Host = ip.String()
				return c
			}
		} else if ip := net.ParseIP(c.Manager.Addr); ip != nil && !net.IPv4zero.Equal(ip) {
			c.Task.Redis.Host = ip.String()
			return c
		}
	}
	c.Task.Redis.Host = ""
	return c
}

type ManagerConfig struct {
	// Addr is manager address.
	Addr string `yaml:"addr" mapstructure:"addr"`

	// SchedulerClusterID is scheduler cluster id.
	SchedulerClusterID uint64 `yaml:"schedulerClusterID" mapstructure:"schedulerClusterID"`

	// KeepAlive configuration
	KeepAlive KeepAliveConfig `yaml:"keepAlive" mapstructure:"keepAlive"`
}

type KeepAliveConfig struct {
	// Keep alive interval
	Interval time.Duration `yaml:"interval" mapstructure:"interval"`

	// Keep alive retry max attempts
	RetryMaxAttempts int `yaml:"retryMaxAttempts" mapstructure:"retryMaxAttempts"`

	// Keep alive retry init backoff
	RetryInitBackOff float64 `yaml:"retryInitBackOff" mapstructure:"retryInitBackOff"`

	// Keep alive retry max backoff
	RetryMaxBackOff float64 `yaml:"retryMaxBackOff" mapstructure:"retryMaxBackOff"`
}

type DynConfig struct {
	// Type is dynconfig source type.
	Type dc.SourceType `yaml:"type" mapstructure:"type"`

	// ExpireTime is expire time for manager cache.
	ExpireTime time.Duration `yaml:"expireTime" mapstructure:"expireTime"`

	// CDNDirPath is cdn dir.
	CDNDirPath string `yaml:"cdnDirPath" mapstructure:"cdnDirPath"`

	// Data is dynconfig local data.
	Data *DynconfigData `yaml:"data" mapstructure:"data"`
}

type SchedulerConfig struct {
	DisableCDN bool   `yaml:"disableCDN" mapstructure:"disableCDN"`
	ABTest     bool   `yaml:"abtest" mapstructure:"abtest"`
	AScheduler string `yaml:"ascheduler" mapstructure:"ascheduler"`
	BScheduler string `yaml:"bscheduler" mapstructure:"bscheduler"`
	WorkerNum  int    `yaml:"workerNum" mapstructure:"workerNum"`
	Monitor    bool   `yaml:"monitor" mapstructure:"monitor"`
	// AccessWindow should less than CDN task expireTime
	AccessWindow         time.Duration `yaml:"accessWindow" mapstructure:"accessWindow"`
	CandidateParentCount int           `yaml:"candidateParentCount" mapstructure:"candidateParentCount"`
	Scheduler            string        `yaml:"scheduler" mapstructure:"scheduler"`
	CDNLoad              int           `yaml:"cDNLoad" mapstructure:"cDNLoad"`
	ClientLoad           int           `yaml:"clientLoad" mapstructure:"clientLoad"`
	OpenMonitor          bool          `yaml:"openMonitor" mapstructure:"openMonitor"`
	GC                   *GCConfig     `yaml:"gc" mapstructure:"gc"`
}

type ServerConfig struct {
	IP   string `yaml:"ip" mapstructure:"ip"`
	Port int    `yaml:"port" mapstructure:"port"`
}

type GCConfig struct {
	PeerGCInterval time.Duration `yaml:"peerGCInterval" mapstructure:"peerGCInterval"`
	TaskGCInterval time.Duration `yaml:"taskGCInterval" mapstructure:"taskGCInterval"`
	PeerTTL        time.Duration
	TaskTTL        time.Duration
}

type HostConfig struct {
	// Peerhost location for scheduler
	Location string `mapstructure:"location" yaml:"location"`

	// Peerhost idc for scheduler
	IDC string `mapstructure:"idc" yaml:"idc"`
}

type RedisConfig struct {
	Host      string `yaml:"host" mapstructure:"host"`
	Port      int    `yaml:"port" mapstructure:"port"`
	Password  string `yaml:"password" mapstructure:"password"`
	BrokerDB  int    `yaml:"brokerDB" mapstructure:"brokerDB"`
	BackendDB int    `yaml:"backendDB" mapstructure:"backendDB"`
}

type TaskConfig struct {
	GlobalWorkerNum    int          `yaml:"globalWorkerNum" mapstructure:"globalWorkerNum"`
	SchedulerWorkerNum int          `yaml:"schedulerWorkerNum" mapstructure:"schedulerWorkerNum"`
	LocalWorkerNum     int          `yaml:"localWorkerNum" mapstructure:"localWorkerNum"`
	Redis              *RedisConfig `yaml:"redis" mapstructure:"redis"`
}
