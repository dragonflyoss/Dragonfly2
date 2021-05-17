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

	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	"github.com/pkg/errors"
)

type Config struct {
	Console   bool                  `yaml:"console"`
	Verbose   bool                  `yaml:"verbose"`
	PProfPort int                   `yaml:"pprofPort"`
	Dynconfig *DynconfigOptions     `yaml:"dynconfig"`
	Manager   *ManagerConfig        `yaml:"manager"`
	Scheduler SchedulerConfig       `yaml:"scheduler"`
	Server    ServerConfig          `yaml:"server"`
	Worker    SchedulerWorkerConfig `yaml:"worker"`
	GC        GCConfig              `yaml:"gc"`
}

func New() *Config {
	return &config
}

func (c *Config) Validate() error {
	if c.Manager != nil {
		if len(c.Manager.NetAddrs) <= 0 {
			return errors.New("empty manager config is not specified")
		}
	}

	return nil
}

type ManagerConfig struct {
	// NetAddrs is manager addresses.
	NetAddrs []dfnet.NetAddr `yaml:"netAddrs"`
}

type DynconfigOptions struct {
	// ExpireTime is expire time for manager cache.
	ExpireTime time.Duration `yaml:"expireTime"`

	// ExpireTime is expire time for manager cache.
	Path string `yaml:"path"`
}

type SchedulerConfig struct {
	ABTest     bool   `yaml:"abtest"`
	AScheduler string `yaml:"ascheduler"`
	BScheduler string `yaml:"bscheduler"`
}

type ServerConfig struct {
	IP   string `yaml:"ip"`
	Port int    `yaml:"port"`
}

type SchedulerWorkerConfig struct {
	WorkerNum         int `yaml:"workerNum"`
	WorkerJobPoolSize int `yaml:"workerJobPoolSize"`
	SenderNum         int `yaml:"senderNum"`
	SenderJobPoolSize int `yaml:"senderJobPoolSize"`
}

type GCConfig struct {
	PeerTaskDelay int64 `yaml:"peerTaskDelay"`
	TaskDelay     int64 `yaml:"taskDelay"`
}
