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
	GC           *GCConfig        `yaml:"gc" mapstructure:"gc"`
	DynConfig    *DynConfig       `yaml:"dynConfig" mapstructure:"dynConfig"`
	Manager      *ManagerConfig   `yaml:"manager" mapstructure:"manager"`
}

func New() *Config {
	return &Config{
		Scheduler: NewDefaultSchedulerConfig(),
		Server:    NewDefaultServerConfig(),
		GC:        NewDefaultGCConfig(),
		DynConfig: NewDefaultDynConfig(),
		Manager:   NewDefaultManagerConfig(),
	}
}

func (c *Config) Validate() error {
	if c.DynConfig.Type == dc.LocalSourceType && c.DynConfig.Path == "" {
		return errors.New("dynconfig is LocalSourceType type requires parameter path")
	}

	if c.DynConfig.Type == dc.ManagerSourceType {
		if c.DynConfig.ExpireTime == 0 {
			return errors.New("dynconfig is ManagerSourceType type requires parameter expireTime")
		}

		if c.DynConfig.CachePath == "" {
			return errors.New("dynconfig is ManagerSourceType type requires parameter cachePath")
		}

		if c.DynConfig.Addr == "" {
			return errors.New("dynconfig is ManagerSourceType type requires parameter addr")
		}
	}

	return nil
}

func NewDefaultDynConfig() *DynConfig {
	return &DynConfig{
		Type:       dc.LocalSourceType,
		ExpireTime: 60000 * 1000 * 1000,
		Path:       SchedulerDynconfigPath,
		CachePath:  SchedulerDynconfigCachePath,
		CDNDirPath: CDNDirCachePath,
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
		EnableCDN:            true,
		ABTest:               false,
		WorkerNum:            runtime.GOMAXPROCS(0),
		WorkerJobPoolSize:    10000,
		SenderNum:            10,
		SenderJobPoolSize:    10000,
		Monitor:              false,
		AccessWindow:         0,
		CandidateParentCount: 0,
		Scheduler:            "basic",
	}
}

func NewDefaultGCConfig() *GCConfig {
	return &GCConfig{
		TaskDelay:     3600 * 1000,
		PeerTaskDelay: 3600 * 1000,
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

	// Addr is dynconfig source address.
	Addr string `yaml:"addr" mapstructure:"addr"`

	// Path is dynconfig filepath.
	Path string `yaml:"path" mapstructure:"path"`

	// CachePath is cache filepath.
	CachePath string `yaml:"cachePath" mapstructure:"cachePath"`

	// CDNDirPath is cdn dir.
	CDNDirPath string `yaml:"cdnDirPath" mapstructure:"cdnDirPath"`
}

type SchedulerConfig struct {
	EnableCDN         bool   `yaml:"enableCDN" mapstructure:"enableCDN"`
	ABTest            bool   `yaml:"abtest" mapstructure:"abtest"`
	AScheduler        string `yaml:"ascheduler" mapstructure:"ascheduler"`
	BScheduler        string `yaml:"bscheduler" mapstructure:"bscheduler"`
	WorkerNum         int    `yaml:"workerNum" mapstructure:"workerNum"`
	WorkerJobPoolSize int    `yaml:"workerJobPoolSize" mapstructure:"workerJobPoolSize"`
	SenderNum         int    `yaml:"senderNum" mapstructure:"senderNum"`
	SenderJobPoolSize int    `yaml:"senderJobPoolSize" mapstructure:"senderJobPoolSize"`
	Monitor           bool   `yaml:"monitor" mapstructure:"monitor"`
	// AccessWindow should less than CDN task expireTime
	AccessWindow         time.Duration `yaml:"accessWindow" mapstructure:"accessWindow"`
	CandidateParentCount int           `yaml:"candidateParentCount" mapstructure:"candidateParentCount"`
	Scheduler            string        `yaml:"scheduler" mapstructure:"scheduler"`
	CDNLoad              int
	ClientLoad           int
}

type ServerConfig struct {
	IP   string `yaml:"ip" mapstructure:"ip"`
	Port int    `yaml:"port" mapstructure:"port"`
}

type GCConfig struct {
	PeerTaskDelay time.Duration `yaml:"peerTaskDelay" mapstructure:"peerTaskDelay"`
	TaskDelay     time.Duration `yaml:"taskDelay" mapstructure:"taskDelay"`
}
