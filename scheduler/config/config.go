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
	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	"d7y.io/dragonfly/v2/pkg/util/net/iputils"
	"github.com/pkg/errors"
)

type Config struct {
	base.Options `yaml:",inline" mapstructure:",squash"`
	ConfigServer string           `yaml:"configServer" mapstructure:"configServer"`
	Scheduler    *SchedulerConfig `yaml:"scheduler" mapstructure:"scheduler"`
	Server       *ServerConfig    `yaml:"server" mapstructure:"server"`
	GC           *GCConfig        `yaml:"gc" mapstructure:"gc"`
	DynConfig    *DynConfig       `yaml:"dynconfig"`
	Manager      *ManagerConfig   `yaml:"manager"`
}

func New() *Config {
	return &Config{
		ConfigServer: "",
		Scheduler:    NewDefaultSchedulerConfig(),
		Server:       NewDefaultServerConfig(),
		GC:           NewDefaultGCConfig(),
		DynConfig:    NewDefaultDynConfig(),
	}
}

func (c *Config) Validate() error {
	if c.Manager != nil {
		if len(c.Manager.NetAddrs) <= 0 {
			return errors.New("empty manager config is not specified")
		}
	}

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

		if len(c.DynConfig.NetAddrs) <= 0 {
			return errors.New("dynconfig is ManagerSourceType type requires parameter netAddrs")
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
		ABTest:            false,
		WorkerNum:         runtime.GOMAXPROCS(0),
		WorkerJobPoolSize: 10000,
		SenderNum:         10,
		SenderJobPoolSize: 10000,
	}
}

func NewDefaultGCConfig() *GCConfig {
	return &GCConfig{
		TaskDelay:     3600 * 1000,
		PeerTaskDelay: 3600 * 1000,
	}
}

type ManagerConfig struct {
	// NetAddrs is manager addresses.
	NetAddrs []dfnet.NetAddr `yaml:"netAddrs"`
}

type DynConfig struct {
	// Type is dynconfig source type.
	Type dc.SourceType `yaml:"type"`

	// ExpireTime is expire time for manager cache.
	ExpireTime time.Duration `yaml:"expireTime"`

	// NetAddrs is dynconfig source addresses.
	NetAddrs []dfnet.NetAddr `yaml:"netAddrs"`

	// Path is dynconfig filepath.
	Path string `yaml:"path"`

	// CachePath is cache filepath.
	CachePath string `yaml:"cachePath"`

	// CDNDirPath is cdn dir.
	CDNDirPath string `yaml:"cdnDirPata"`
}

type SchedulerConfig struct {
	ABTest            bool   `yaml:"abtest" mapstructure:"abtest"`
	AScheduler        string `yaml:"ascheduler" mapstructure:"ascheduler"`
	BScheduler        string `yaml:"bscheduler" mapstructure:"bscheduler"`
	WorkerNum         int    `yaml:"workerNum" mapstructure:"workerNum"`
	WorkerJobPoolSize int    `yaml:"workerJobPoolSize" mapstructure:"workerJobPoolSize"`
	SenderNum         int    `yaml:"senderNum" mapstructure:"senderNum"`
	SenderJobPoolSize int    `yaml:"senderJobPoolSize" mapstructure:"senderJobPoolSize"`
}

type ServerConfig struct {
	IP   string `yaml:"ip" mapstructure:"ip"`
	Port int    `yaml:"port" mapstructure:"port"`
}

type GCConfig struct {
	PeerTaskDelay time.Duration `yaml:"peerTaskDelay" mapstructure:"peerTaskDelay"`
	TaskDelay     time.Duration `yaml:"taskDelay" mapstructure:"taskDelay"`
}
