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

import "runtime"

const (
	DefaultConfigFilePath string = "/etc/dragonfly/scheduler.yaml"
)

type Config struct {
	Console   bool                  `mapstructure:"console"`
	Verbose   bool                  `mapstructure:"verbose"`
	PProfPort int                   `mapstructure:"pprofPort"`
	Scheduler SchedulerConfig       `mapstructure:"scheduler"`
	Server    ServerConfig          `mapstructure:"server"`
	Worker    SchedulerWorkerConfig `mapstructure:"worker"`
	CDN       CDNConfig             `mapstructure:"cdn"`
	GC        GCConfig              `mapstructure:"gc"`
}

type SchedulerConfig struct {
	ABTest     bool   `mapstructure:"abtest"`
	AScheduler string `mapstructure:"ascheduler"`
	BScheduler string `mapstructure:"bscheduler"`
}

type ServerConfig struct {
	IP   string `mapstructure:"ip"`
	Port int    `mapstructure:"port"`
}

type SchedulerWorkerConfig struct {
	WorkerNum         int `mapstructure:"workerNum"`
	WorkerJobPoolSize int `mapstructure:"workerJobPoolSize"`
	SenderNum         int `mapstructure:"senderNum"`
	SenderJobPoolSize int `mapstructure:"senderJobPoolSize"`
}

type CDNServerConfig struct {
	Name         string `mapstructure:"name"`
	IP           string `mapstructure:"ip"`
	RpcPort      int    `mapstructure:"rpcPort"`
	DownloadPort int    `mapstructure:"downloadPort"`
}

type CDNConfig struct {
	Servers []CDNServerConfig `mapstructure:"servers"`
}

type GCConfig struct {
	PeerTaskDelay int64 `mapstructure:"peerTaskDelay"`
	TaskDelay     int64 `mapstructure:"taskDelay"`
}

func New() *Config {
	return &config
}

func GetConfig() *Config {
	return &config
}

func createDefaultConfig() *Config {
	return &Config{
		Console: false,
		Server: ServerConfig{
			Port: 8002,
		},
		Worker: SchedulerWorkerConfig{
			WorkerNum:         runtime.GOMAXPROCS(0),
			WorkerJobPoolSize: 10000,
			SenderNum:         10,
			SenderJobPoolSize: 10000,
		},
		Scheduler: SchedulerConfig{
			ABTest: false,
		},
		CDN: CDNConfig{
			Servers: []CDNServerConfig{
				{
					Name:         "cdn",
					IP:           "127.0.0.1",
					RpcPort:      8003,
					DownloadPort: 8001,
				},
			},
		},
		GC: GCConfig{
			TaskDelay:     3600 * 1000,
			PeerTaskDelay: 3600 * 1000,
		},
	}
}
