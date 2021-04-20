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

var config = createDefaultConfig()

type Config struct {
	Debug     bool                  `mapstructure:"debug"`
	Console   bool                  `mapstructure:"console"`
	Verbose   bool                  `mapstructure:"verbose"`
	Scheduler schedulerConfig       `mapstructure:"scheduler"`
	Server    serverConfig          `mapstructure:"server"`
	Worker    schedulerWorkerConfig `mapstructure:"worker"`
	CDN       cdnConfig             `mapstructure:"cdn"`
	GC        gcConfig              `mapstructure:"gc"`
}

type schedulerConfig struct {
	ABTest     bool   `mapstructure:"abtest"`
	AScheduler string `mapstructure:"ascheduler"`
	BScheduler string `mapstructure:"bscheduler"`
}

type serverConfig struct {
	IP   string `mapstructure:"ip"`
	Port int    `mapstructure:"port"`
}

type schedulerWorkerConfig struct {
	WorkerNum         int `mapstructure:"workerNum"`
	WorkerJobPoolSize int `mapstructure:"workerJobPoolSize"`
	SenderNum         int `mapstructure:"senderNum"`
	SenderJobPoolSize int `mapstructure:"senderJobPoolSize"`
}

type CdnServerConfig struct {
	Name         string `mapstructure:"name"`
	IP           string `mapstructure:"ip"`
	RpcPort      int    `mapstructure:"rpcPort"`
	DownloadPort int    `mapstructure:"downloadPort"`
}

type cdnConfig struct {
	Servers []CdnServerConfig `mapstructure:"servers"`
}

type gcConfig struct {
	PeerTaskDelay int64 `mapstructure:"peerTaskDelay"`
	TaskDelay     int64 `mapstructure:"taskDelay"`
}

func GetConfig() *Config {
	return config
}

func SetConfig(cfg *Config) {
	config = cfg
}

func createDefaultConfig() *Config {
	return &Config{
		Debug:   false,
		Console: false,
		Server: serverConfig{
			Port: 8002,
		},
		Worker: schedulerWorkerConfig{
			WorkerNum:         runtime.GOMAXPROCS(0),
			WorkerJobPoolSize: 10000,
			SenderNum:         10,
			SenderJobPoolSize: 10000,
		},
		Scheduler: schedulerConfig{
			ABTest: false,
		},
		CDN: cdnConfig{
			Servers: []CdnServerConfig{
				{
					Name:         "cdn",
					IP:           "127.0.0.1",
					RpcPort:      8003,
					DownloadPort: 8001,
				},
			},
		},
		GC: gcConfig{
			TaskDelay:     3600 * 1000,
			PeerTaskDelay: 3600 * 1000,
		},
	}
}
