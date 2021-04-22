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
	Debug     bool                  `yaml:"debug" mapstructure:",squash"`
	Console   bool                  `yaml:"console"`
	Scheduler schedulerConfig       `yaml:"scheduler" mapstructure:",squash"`
	Server    serverConfig          `yaml:"server" mapstructure:",squash"`
	Worker    schedulerWorkerConfig `yaml:"worker" mapstructure:",squash"`
	CDN       cdnConfig             `yaml:"cdn" mapstructure:",squash"`
	GC        gcConfig              `yaml:"gc" mapstructure:",squash"`
}

type schedulerConfig struct {
	ABTest     bool
	AScheduler string
	BScheduler string
}

type serverConfig struct {
	IP   string `yaml:"ip",omitempty`
	Port int    `yaml:"port"`
}

type schedulerWorkerConfig struct {
	WorkerNum         int `yaml:"worker-num"`
	WorkerJobPoolSize int `yaml:"worker-job-pool-size"`
	SenderNum         int `yaml:"sender-num"`
	SenderJobPoolSize int `yaml:"sender-job-pool-size"`
}

type CdnServerConfig struct {
	CdnName      string `yaml:"name"`
	IP           string `yaml:"ip",omitempty`
	RpcPort      int    `yaml:"rpc-port"`
	DownloadPort int    `yaml:"download-port"`
}

type cdnConfig struct {
	List []CdnServerConfig `yaml:"servers" mapstructure:",squash"`
}

type gcConfig struct {
	PeerTaskDelay int64 `yaml:"peer-task-gc-delay-time"`
	TaskDelay     int64 `yaml:"task-gc-delay-time"`
}

func GetConfig() *Config {
	return config
}

func SetConfig(cfg *Config) {
	config = cfg
}

func createDefaultConfig() *Config {
	return &Config{
		Debug:   true,
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
			List: []CdnServerConfig{
				{
					CdnName:      "cdn",
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
