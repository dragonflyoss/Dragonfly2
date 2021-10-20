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
	Job          *JobConfig       `yaml:"job" mapstructure:"job"`
	Metrics      *RestConfig      `yaml:"metrics" mapstructure:"metrics"`
	DisableCDN   bool             `yaml:"disableCDN" mapstructure:"disableCDN"`
}

func New() *Config {
	return &Config{
		Scheduler: &SchedulerConfig{
			ABTest:               false,
			AEvaluator:           "",
			BEvaluator:           "",
			WorkerNum:            runtime.GOMAXPROCS(0),
			BackSourceCount:      3,
			AccessWindow:         3 * time.Minute,
			CandidateParentCount: 10,
			Scheduler:            "basic",
			CDNLoad:              100,
			ClientLoad:           10,
			OpenMonitor:          false,
			GC: &GCConfig{
				PeerGCInterval: 1 * time.Minute,
				TaskGCInterval: 1 * time.Minute,
				PeerTTL:        10 * time.Minute,
				PeerTTI:        3 * time.Minute,
				TaskTTL:        10 * time.Minute,
				TaskTTI:        3 * time.Minute,
			},
		},
		Server: &ServerConfig{
			IP:   iputils.HostIP,
			Host: iputils.HostName,
			Port: 8002,
		},
		DynConfig: &DynConfig{
			Type:       dc.LocalSourceType,
			ExpireTime: 30 * time.Second,
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
					},
				},
			},
		},
		Manager: &ManagerConfig{
			Addr:               "",
			SchedulerClusterID: 0,
			KeepAlive: KeepAliveConfig{
				Interval: 5 * time.Second,
			},
		},
		Host: &HostConfig{
			Location: "",
			IDC:      "",
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
		DisableCDN: false,
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

		if c.Manager.SchedulerClusterID == 0 {
			return errors.New("dynconfig is ManagerSourceType type requires parameter manager schedulerClusterID")
		}
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
	ABTest          bool   `yaml:"abtest" mapstructure:"abtest"`
	AEvaluator      string `yaml:"aevaluator" mapstructure:"aevaluator"`
	BEvaluator      string `yaml:"bevaluator" mapstructure:"bevaluator"`
	WorkerNum       int    `yaml:"workerNum" mapstructure:"workerNum"`
	BackSourceCount int32  `yaml:"backSourceCount" mapstructure:"backSourceCount"`
	// AccessWindow should less than CDN task expireTime
	AccessWindow         time.Duration `yaml:"accessWindow" mapstructure:"accessWindow"`
	CandidateParentCount int           `yaml:"candidateParentCount" mapstructure:"candidateParentCount"`
	Scheduler            string        `yaml:"scheduler" mapstructure:"scheduler"`
	CDNLoad              int           `yaml:"cdnLoad" mapstructure:"cdnLoad"`
	ClientLoad           int32         `yaml:"clientLoad" mapstructure:"clientLoad"`
	OpenMonitor          bool          `yaml:"openMonitor" mapstructure:"openMonitor"`
	GC                   *GCConfig     `yaml:"gc" mapstructure:"gc"`
}

type ServerConfig struct {
	IP   string `yaml:"ip" mapstructure:"ip"`
	Host string `yaml:"host" mapstructure:"host"`
	Port int    `yaml:"port" mapstructure:"port"`
}

type GCConfig struct {
	PeerGCInterval time.Duration `yaml:"peerGCInterval" mapstructure:"peerGCInterval"`
	// PeerTTL is advised to set the time to be smaller than the expire time of a task in the CDN
	PeerTTL        time.Duration `yaml:"peerTTL" mapstructure:"peerTTL"`
	PeerTTI        time.Duration `yaml:"peerTTI" mapstructure:"peerTTI"`
	TaskGCInterval time.Duration `yaml:"taskGCInterval" mapstructure:"taskGCInterval"`
	TaskTTL        time.Duration `yaml:"taskTTL" mapstructure:"taskTTL"`
	TaskTTI        time.Duration `yaml:"taskTTI" mapstructure:"taskTTI"`
}

type RestConfig struct {
	Addr string `yaml:"addr" mapstructure:"addr"`
}

type HostConfig struct {
	// Location for scheduler
	Location string `mapstructure:"location" yaml:"location"`

	// IDC for scheduler
	IDC string `mapstructure:"idc" yaml:"idc"`
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
