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
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

var (
	mockManagerConfig = ManagerConfig{
		Addr:               "localhost",
		SchedulerClusterID: DefaultManagerSchedulerClusterID,
		KeepAlive: KeepAliveConfig{
			Interval: DefaultManagerKeepAliveInterval,
		},
	}

	mockJobConfig = JobConfig{
		Enable:             true,
		GlobalWorkerNum:    DefaultJobGlobalWorkerNum,
		SchedulerWorkerNum: DefaultJobSchedulerWorkerNum,
		LocalWorkerNum:     DefaultJobLocalWorkerNum,
	}

	mockMetricsConfig = MetricsConfig{
		Enable: true,
		Addr:   DefaultMetricsAddr,
	}

	mockRedisConfig = RedisConfig{
		Addrs:      []string{"127.0.0.0:6379"},
		MasterName: "master",
		Username:   "baz",
		Password:   "bax",
		BrokerDB:   DefaultRedisBrokerDB,
		BackendDB:  DefaultRedisBackendDB,
	}
)

func TestConfig_Load(t *testing.T) {
	config := &Config{
		Scheduler: SchedulerConfig{
			Algorithm:              "default",
			BackToSourceCount:      3,
			RetryBackToSourceLimit: 2,
			RetryLimit:             10,
			RetryInterval:          10 * time.Second,
			GC: GCConfig{
				PieceDownloadTimeout: 5 * time.Second,
				PeerGCInterval:       10 * time.Second,
				PeerTTL:              1 * time.Minute,
				TaskGCInterval:       30 * time.Second,
				HostGCInterval:       1 * time.Minute,
				HostTTL:              1 * time.Minute,
			},
		},
		Server: ServerConfig{
			AdvertiseIP:   net.ParseIP("127.0.0.1"),
			AdvertisePort: 8004,
			ListenIP:      net.ParseIP("0.0.0.0"),
			Port:          8002,
			Host:          "foo",
			TLS: &GRPCTLSServerConfig{
				CACert: "foo",
				Cert:   "foo",
				Key:    "foo",
			},
			CacheDir:      "foo",
			LogDir:        "foo",
			LogMaxSize:    512,
			LogMaxAge:     5,
			LogMaxBackups: 3,
			PluginDir:     "foo",
			DataDir:       "foo",
		},
		Database: DatabaseConfig{
			Redis: RedisConfig{
				Host:       "127.0.0.1",
				Password:   "foo",
				Addrs:      []string{"foo", "bar"},
				MasterName: "baz",
				Port:       6379,
				BrokerDB:   DefaultRedisBrokerDB,
				BackendDB:  DefaultRedisBackendDB,
			},
		},
		DynConfig: DynConfig{
			RefreshInterval: 10 * time.Second,
		},
		Manager: ManagerConfig{
			Addr: "127.0.0.1:65003",
			TLS: &GRPCTLSClientConfig{
				CACert: "foo",
				Cert:   "foo",
				Key:    "foo",
			},
			SchedulerClusterID: 1,
			KeepAlive: KeepAliveConfig{
				Interval: 5 * time.Second,
			},
		},
		SeedPeer: SeedPeerConfig{
			Enable: true,
			TLS: &GRPCTLSClientConfig{
				CACert: "foo",
				Cert:   "foo",
				Key:    "foo",
			},
			TaskDownloadTimeout: 12 * time.Hour,
		},
		Host: HostConfig{
			IDC:      "foo",
			Location: "baz",
		},
		Job: JobConfig{
			Enable:             true,
			GlobalWorkerNum:    1,
			SchedulerWorkerNum: 1,
			LocalWorkerNum:     5,
		},
		Storage: StorageConfig{
			MaxSize:    1,
			MaxBackups: 1,
			BufferSize: 1,
		},
		Metrics: MetricsConfig{
			Enable:     false,
			Addr:       ":8000",
			EnableHost: true,
		},
		Network: NetworkConfig{
			EnableIPv6: true,
		},
	}

	schedulerConfigYAML := &Config{}
	contentYAML, _ := os.ReadFile("./testdata/scheduler.yaml")
	if err := yaml.Unmarshal(contentYAML, &schedulerConfigYAML); err != nil {
		t.Fatal(err)
	}

	assert := assert.New(t)
	assert.EqualValues(schedulerConfigYAML, config)
}

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name   string
		config *Config
		mock   func(cfg *Config)
		expect func(t *testing.T, err error)
	}{
		{
			name:   "valid config",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job = mockJobConfig
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
		{
			name:   "server requires parameter advertiseIP",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Job = mockJobConfig
				cfg.Server.AdvertiseIP = nil
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "server requires parameter advertiseIP")
			},
		},
		{
			name:   "server requires parameter advertisePort",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Job = mockJobConfig
				cfg.Server.AdvertisePort = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "server requires parameter advertisePort")
			},
		},
		{
			name:   "server requires parameter listenIP",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Job = mockJobConfig
				cfg.Server.ListenIP = nil
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "server requires parameter listenIP")
			},
		},
		{
			name:   "server requires parameter port",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Job = mockJobConfig
				cfg.Server.Port = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "server requires parameter port")
			},
		},
		{
			name:   "server requires parameter host",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Job = mockJobConfig
				cfg.Server.Host = ""
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "server requires parameter host")
			},
		},
		{
			name:   "server tls requires parameter caCert",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Server.TLS = &GRPCTLSServerConfig{
					CACert: "",
					Cert:   "foo",
					Key:    "foo",
				}
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "server tls requires parameter caCert")
			},
		},
		{
			name:   "server tls requires parameter cert",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Server.TLS = &GRPCTLSServerConfig{
					CACert: "foo",
					Cert:   "",
					Key:    "foo",
				}
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "server tls requires parameter cert")
			},
		},
		{
			name:   "server tls requires parameter key",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Server.TLS = &GRPCTLSServerConfig{
					CACert: "foo",
					Cert:   "foo",
					Key:    "",
				}
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "server tls requires parameter key")
			},
		},
		{
			name:   "redis requires parameter brokerDB",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Database.Redis.BrokerDB = -1
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "redis requires parameter brokerDB")
			},
		},
		{
			name:   "redis requires parameter backendDB",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Database.Redis.BackendDB = -1
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "redis requires parameter backendDB")
			},
		},
		{
			name:   "scheduler requires parameter algorithm",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job = mockJobConfig
				cfg.Scheduler.Algorithm = ""
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "scheduler requires parameter algorithm")
			},
		},
		{
			name:   "scheduler requires parameter backToSourceCount",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job = mockJobConfig
				cfg.Scheduler.BackToSourceCount = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "scheduler requires parameter backToSourceCount")
			},
		},
		{
			name:   "scheduler requires parameter retryBackToSourceLimit",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job = mockJobConfig
				cfg.Scheduler.RetryBackToSourceLimit = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "scheduler requires parameter retryBackToSourceLimit")
			},
		},
		{
			name:   "scheduler requires parameter retryLimit",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job = mockJobConfig
				cfg.Scheduler.RetryLimit = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "scheduler requires parameter retryLimit")
			},
		},
		{
			name:   "scheduler requires parameter retryInterval",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job = mockJobConfig
				cfg.Scheduler.RetryInterval = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "scheduler requires parameter retryInterval")
			},
		},
		{
			name:   "scheduler requires parameter pieceDownloadTimeout",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job = mockJobConfig
				cfg.Scheduler.GC.PieceDownloadTimeout = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "scheduler requires parameter pieceDownloadTimeout")
			},
		},
		{
			name:   "scheduler requires parameter peerTTL",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job = mockJobConfig
				cfg.Scheduler.GC.PeerTTL = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "scheduler requires parameter peerTTL")
			},
		},
		{
			name:   "scheduler requires parameter peerGCInterval",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job = mockJobConfig
				cfg.Scheduler.GC.PeerGCInterval = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "scheduler requires parameter peerGCInterval")
			},
		},
		{
			name:   "scheduler requires parameter taskGCInterval",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job = mockJobConfig
				cfg.Scheduler.GC.TaskGCInterval = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "scheduler requires parameter taskGCInterval")
			},
		},
		{
			name:   "scheduler requires parameter hostGCInterval",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job = mockJobConfig
				cfg.Scheduler.GC.HostGCInterval = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "scheduler requires parameter hostGCInterval")
			},
		},
		{
			name:   "scheduler requires parameter hostTTL",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job = mockJobConfig
				cfg.Scheduler.GC.HostTTL = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "scheduler requires parameter hostTTL")
			},
		},
		{
			name:   "dynconfig requires parameter refreshInterval",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job = mockJobConfig
				cfg.DynConfig.RefreshInterval = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "dynconfig requires parameter refreshInterval")
			},
		},
		{
			name:   "manager requires parameter addr",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job = mockJobConfig
				cfg.Manager.Addr = ""
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "manager requires parameter addr")
			},
		},
		{
			name:   "manager requires parameter schedulerClusterID",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job = mockJobConfig
				cfg.Manager.SchedulerClusterID = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "manager requires parameter schedulerClusterID")
			},
		},
		{
			name:   "manager requires parameter keepAlive interval",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job = mockJobConfig
				cfg.Manager.KeepAlive.Interval = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "manager requires parameter keepAlive interval")
			},
		},
		{
			name:   "manager tls requires parameter caCert",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job = mockJobConfig
				cfg.Manager.TLS = &GRPCTLSClientConfig{
					CACert: "",
					Cert:   "foo",
					Key:    "foo",
				}
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "manager tls requires parameter caCert")
			},
		},
		{
			name:   "manager tls requires parameter cert",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Manager.TLS = &GRPCTLSClientConfig{
					CACert: "foo",
					Cert:   "",
					Key:    "foo",
				}
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "manager tls requires parameter cert")
			},
		},
		{
			name:   "manager tls requires parameter key",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Manager.TLS = &GRPCTLSClientConfig{
					CACert: "foo",
					Cert:   "foo",
					Key:    "",
				}
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "manager tls requires parameter key")
			},
		},
		{
			name:   "seedPeer requires parameter taskDownloadTimeout",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job = mockJobConfig
				cfg.SeedPeer.TaskDownloadTimeout = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "seedPeer requires parameter taskDownloadTimeout")
			},
		},
		{
			name:   "seedPeer tls requires parameter caCert",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job = mockJobConfig
				cfg.SeedPeer.TLS = &GRPCTLSClientConfig{
					CACert: "",
					Cert:   "foo",
					Key:    "foo",
				}
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "seedPeer tls requires parameter caCert")
			},
		},
		{
			name:   "seedPeer tls requires parameter cert",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job = mockJobConfig
				cfg.SeedPeer.TLS = &GRPCTLSClientConfig{
					CACert: "foo",
					Cert:   "",
					Key:    "foo",
				}
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "seedPeer tls requires parameter cert")
			},
		},
		{
			name:   "seedPeer tls requires parameter key",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job = mockJobConfig
				cfg.SeedPeer.TLS = &GRPCTLSClientConfig{
					CACert: "foo",
					Cert:   "foo",
					Key:    "",
				}
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "seedPeer tls requires parameter key")
			},
		},
		{
			name:   "job requires parameter globalWorkerNum",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job = mockJobConfig
				cfg.Job.GlobalWorkerNum = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "job requires parameter globalWorkerNum")
			},
		},
		{
			name:   "job requires parameter schedulerWorkerNum",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job = mockJobConfig
				cfg.Job.SchedulerWorkerNum = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "job requires parameter schedulerWorkerNum")
			},
		},
		{
			name:   "job requires parameter localWorkerNum",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job = mockJobConfig
				cfg.Job.LocalWorkerNum = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "job requires parameter localWorkerNum")
			},
		},
		{
			name:   "storage requires parameter maxSize",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job = mockJobConfig
				cfg.Storage.MaxSize = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "storage requires parameter maxSize")
			},
		},
		{
			name:   "storage requires parameter maxBackups",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job = mockJobConfig
				cfg.Storage.MaxBackups = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "storage requires parameter maxBackups")
			},
		},
		{
			name:   "storage requires parameter bufferSize",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job = mockJobConfig
				cfg.Storage.BufferSize = -1
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "storage requires parameter bufferSize")
			},
		},
		{
			name:   "metrics requires parameter addr",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job = mockJobConfig
				cfg.Metrics = mockMetricsConfig
				cfg.Metrics.Addr = ""
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "metrics requires parameter addr")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.config.Convert(); err != nil {
				t.Fatal(err)
			}

			tc.mock(tc.config)
			tc.expect(t, tc.config.Validate())
		})
	}
}
