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

	testifyassert "github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

func TestConfig_Load(t *testing.T) {
	assert := testifyassert.New(t)

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
				PeerTTL:              60 * time.Second,
				TaskGCInterval:       30 * time.Second,
				HostGCInterval:       1 * time.Minute,
			},
		},
		Server: ServerConfig{
			AdvertiseIP: net.ParseIP("127.0.0.1"),
			ListenIP:    net.ParseIP("0.0.0.0"),
			Port:        8002,
			Host:        "foo",
			WorkHome:    "foo",
			CacheDir:    "foo",
			LogDir:      "foo",
			PluginDir:   "foo",
			DataDir:     "foo",
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
			Enable: true,
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
			Redis: RedisConfig{
				Addrs:      []string{"foo", "bar"},
				MasterName: "baz",
				Host:       "127.0.0.1",
				Port:       6379,
				Password:   "foo",
				BrokerDB:   1,
				BackendDB:  2,
			},
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
			SyncInterval:    30 * time.Second,
			CollectInterval: 60 * time.Second,
			Probe: ProbeConfig{
				QueueLength:  5,
				SyncInterval: 30 * time.Second,
				SyncCount:    50,
			},
		},
	}

	schedulerConfigYAML := &Config{}
	contentYAML, _ := os.ReadFile("./testdata/scheduler.yaml")
	if err := yaml.Unmarshal(contentYAML, &schedulerConfigYAML); err != nil {
		t.Fatal(err)
	}

	assert.EqualValues(schedulerConfigYAML, config)
}
