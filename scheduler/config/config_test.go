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

	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/types"
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

	mockSecurityConfig = SecurityConfig{
		AutoIssueCert: true,
		CACert:        types.PEMContent("foo"),
		TLSPolicy:     rpc.PreferTLSPolicy,
		CertSpec: CertSpec{
			DNSNames:       DefaultCertDNSNames,
			IPAddresses:    DefaultCertIPAddresses,
			ValidityPeriod: DefaultCertValidityPeriod,
		},
	}

	mockRedisConfig = RedisConfig{
		Addrs:             []string{"127.0.0.0:6379"},
		MasterName:        "master",
		Username:          "baz",
		Password:          "bax",
		BrokerDB:          DefaultRedisBrokerDB,
		BackendDB:         DefaultRedisBackendDB,
		NetworkTopologyDB: DefaultNetworkTopologyDB,
	}
)

func TestConfig_Load(t *testing.T) {
	config := &Config{
		Scheduler: SchedulerConfig{
			Algorithm:              "default",
			MaxScheduleCount:       12,
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
			WorkHome:      "foo",
			CacheDir:      "foo",
			LogDir:        "foo",
			PluginDir:     "foo",
			DataDir:       "foo",
		},
		Database: DatabaseConfig{
			Redis: RedisConfig{
				Host:              "127.0.0.1",
				Password:          "foo",
				Addrs:             []string{"foo", "bar"},
				MasterName:        "baz",
				Port:              6379,
				BrokerDB:          DefaultRedisBrokerDB,
				BackendDB:         DefaultRedisBackendDB,
				NetworkTopologyDB: DefaultNetworkTopologyDB,
			},
		},
		Resource: ResourceConfig{
			Task: TaskConfig{
				DownloadTiny: DownloadTinyConfig{
					Scheme:  DefaultResourceTaskDownloadTinyScheme,
					Timeout: DefaultResourceTaskDownloadTinyTimeout,
					TLS: DownloadTinyTLSClientConfig{
						InsecureSkipVerify: true,
					},
				},
			},
		},
		DynConfig: DynConfig{
			RefreshInterval: 10 * time.Second,
		},
		Manager: ManagerConfig{
			Addr:               "127.0.0.1:65003",
			SchedulerClusterID: 1,
			KeepAlive: KeepAliveConfig{
				Interval: 5 * time.Second,
			},
		},
		SeedPeer: SeedPeerConfig{
			Enable:              true,
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
		Security: SecurityConfig{
			AutoIssueCert: true,
			CACert:        "foo",
			TLSVerify:     true,
			TLSPolicy:     "force",
			CertSpec: CertSpec{
				DNSNames:       []string{"foo"},
				IPAddresses:    []net.IP{net.IPv4zero},
				ValidityPeriod: 10 * time.Minute,
			},
		},
		Network: NetworkConfig{
			EnableIPv6: true,
		},
		NetworkTopology: NetworkTopologyConfig{
			Enable:          true,
			CollectInterval: 60 * time.Second,
			Probe: ProbeConfig{
				QueueLength: 5,
				Count:       10,
			},
			Cache: CacheConfig{
				Interval: 5 * time.Minute,
				TTL:      5 * time.Minute,
			},
		},
		Trainer: TrainerConfig{
			Enable:        false,
			Addr:          "127.0.0.1:9090",
			Interval:      10 * time.Minute,
			UploadTimeout: 2 * time.Hour,
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
			name:   "redis requires parameter networkTopologyDB",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Database.Redis.NetworkTopologyDB = -1
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "redis requires parameter networkTopologyDB")
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
			name:   "downloadTiny requires parameter scheme",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job = mockJobConfig
				cfg.Resource.Task.DownloadTiny.Scheme = ""
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "downloadTiny requires parameter scheme")
			},
		},
		{
			name:   "downloadTiny requires parameter timeout",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job = mockJobConfig
				cfg.Resource.Task.DownloadTiny.Timeout = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "downloadTiny requires parameter timeout")
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
				cfg.Storage.BufferSize = 0
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
		{
			name:   "security requires parameter caCert",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job = mockJobConfig
				cfg.Security = mockSecurityConfig
				cfg.Security.CACert = ""
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "security requires parameter caCert")
			},
		},
		{
			name:   "security requires parameter tlsPolicy",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job = mockJobConfig
				cfg.Security = mockSecurityConfig
				cfg.Security.TLSPolicy = ""
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "security requires parameter tlsPolicy")
			},
		},
		{
			name:   "certSpec requires parameter ipAddresses",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job = mockJobConfig
				cfg.Security = mockSecurityConfig
				cfg.Security.CertSpec.IPAddresses = []net.IP{}
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "certSpec requires parameter ipAddresses")
			},
		},
		{
			name:   "certSpec requires parameter dnsNames",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job = mockJobConfig
				cfg.Security = mockSecurityConfig
				cfg.Security.CertSpec.DNSNames = []string{}
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "certSpec requires parameter dnsNames")
			},
		},
		{
			name:   "certSpec requires parameter validityPeriod",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job = mockJobConfig
				cfg.Security = mockSecurityConfig
				cfg.Security.CertSpec.ValidityPeriod = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "certSpec requires parameter validityPeriod")
			},
		},
		{
			name:   "networkTopology requires parameter collectInterval",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job = mockJobConfig
				cfg.NetworkTopology.CollectInterval = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "networkTopology requires parameter collectInterval")
			},
		},
		{
			name:   "networkTopology requires parameter interval",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job = mockJobConfig
				cfg.NetworkTopology.Cache.Interval = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "networkTopology requires parameter interval")
			},
		},
		{
			name:   "networkTopology requires parameter ttl",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job = mockJobConfig
				cfg.NetworkTopology.Cache.TTL = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "networkTopology requires parameter ttl")
			},
		},
		{
			name:   "probe requires parameter queueLength",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job = mockJobConfig
				cfg.NetworkTopology.Probe.QueueLength = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "probe requires parameter queueLength")
			},
		},
		{
			name:   "probe requires parameter count",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job = mockJobConfig
				cfg.NetworkTopology.Probe.Count = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "probe requires parameter count")
			},
		},
		{
			name:   "trainer requires parameter addr",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job = mockJobConfig
				cfg.Trainer.Enable = true
				cfg.Trainer.Addr = ""
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "trainer requires parameter addr")
			},
		},
		{
			name:   "trainer requires parameter interval",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job = mockJobConfig
				cfg.Trainer.Enable = true
				cfg.Trainer.Interval = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "trainer requires parameter interval")
			},
		},
		{
			name:   "trainer requires parameter interval",
			config: New(),
			mock: func(cfg *Config) {
				cfg.Manager = mockManagerConfig
				cfg.Database.Redis = mockRedisConfig
				cfg.Job = mockJobConfig
				cfg.Trainer.Enable = true
				cfg.Trainer.UploadTimeout = 0
			},
			expect: func(t *testing.T, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "trainer requires parameter uploadTimeout")
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
