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

const (
	DefaultConfigFilePath string = "/etc/dragonfly/scheduler.yaml"
)

type Config struct {
	Console   bool                  `yaml:"console"`
	Verbose   bool                  `yaml:"verbose"`
	PProfPort int                   `yaml:"pprofPort"`
	Jaeger    string                `yaml:"jaeger"`
	Scheduler SchedulerConfig       `yaml:"scheduler"`
	Server    ServerConfig          `yaml:"server"`
	Worker    SchedulerWorkerConfig `yaml:"worker"`
	CDN       CDNConfig             `yaml:"cdn"`
	GC        GCConfig              `yaml:"gc"`
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

type CDNServerConfig struct {
	Name         string `yaml:"name"`
	IP           string `yaml:"ip"`
	RpcPort      int    `yaml:"rpcPort"`
	DownloadPort int    `yaml:"downloadPort"`
}

type CDNConfig struct {
	Servers []CDNServerConfig `yaml:"servers"`
}

type GCConfig struct {
	PeerTaskDelay int64 `yaml:"peerTaskDelay"`
	TaskDelay     int64 `yaml:"taskDelay"`
}

func New() *Config {
	return &config
}
