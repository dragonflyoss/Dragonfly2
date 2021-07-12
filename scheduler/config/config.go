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

	"d7y.io/dragonfly.v2/cmd/dependency/base"
	dc "d7y.io/dragonfly.v2/internal/dynconfig"
	"github.com/pkg/errors"
)

type Config struct {
	base.Options `yaml:",inline" mapstructure:",squash"`
	Scheduler    SchedulerConfig       `yaml:"scheduler" mapstructure:"scheduler"`
	Server       ServerConfig          `yaml:"server" mapstructure:"server"`
	Worker       SchedulerWorkerConfig `yaml:"worker" mapstructure:"worker"`
	GC           GCConfig              `yaml:"gc" mapstructure:"gc"`
	Dynconfig    *DynconfigOptions     `yaml:"dynconfig" mapstructure:"dynconfig"`
	Manager      ManagerConfig         `yaml:"manager" mapstructure:"manager"`
}

func New() *Config {
	return &config
}

func (c *Config) Validate() error {
	if c.Dynconfig.Type == dc.LocalSourceType && c.Dynconfig.Path == "" {
		return errors.New("dynconfig is LocalSourceType type requires parameter path")
	}

	if c.Dynconfig.Type == dc.ManagerSourceType {
		if c.Dynconfig.ExpireTime == 0 {
			return errors.New("dynconfig is ManagerSourceType type requires parameter expireTime")
		}

		if c.Dynconfig.CachePath == "" {
			return errors.New("dynconfig is ManagerSourceType type requires parameter cachePath")
		}

		if c.Dynconfig.Addr == "" {
			return errors.New("dynconfig is ManagerSourceType type requires parameter addr")
		}
	}

	return nil
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

type DynconfigOptions struct {
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
	ABTest     bool   `yaml:"abtest" mapstructure:"abtest"`
	AScheduler string `yaml:"ascheduler" mapstructure:"ascheduler"`
	BScheduler string `yaml:"bscheduler" mapstructure:"bscheduler"`
}

type ServerConfig struct {
	IP   string `yaml:"ip" mapstructure:"ip"`
	Port int    `yaml:"port" mapstructure:"port"`
}

type SchedulerWorkerConfig struct {
	WorkerNum         int `yaml:"workerNum" mapstructure:"workerNum"`
	WorkerJobPoolSize int `yaml:"workerJobPoolSize" mapstructure:"workerJobPoolSize"`
	SenderNum         int `yaml:"senderNum" mapstructure:"senderNum"`
	SenderJobPoolSize int `yaml:"senderJobPoolSize" mapstructure:"senderJobPoolSize"`
}

type GCConfig struct {
	PeerTaskDelay int64 `yaml:"peerTaskDelay" mapstructure:"peerTaskDelay"`
	TaskDelay     int64 `yaml:"taskDelay" mapstructure:"taskDelay"`
}
