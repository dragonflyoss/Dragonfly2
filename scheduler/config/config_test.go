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
	"os"
	"testing"
	"time"

	"github.com/mitchellh/mapstructure"
	testifyassert "github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"

	"d7y.io/dragonfly/v2/pkg/net/fqdn"
	"d7y.io/dragonfly/v2/pkg/net/ip"
	"d7y.io/dragonfly/v2/scheduler/storage"
)

func TestConfig_Load(t *testing.T) {
	assert := testifyassert.New(t)

	config := &Config{
		Scheduler: &SchedulerConfig{
			Algorithm:            "default",
			BackSourceCount:      3,
			RetryBackSourceLimit: 2,
			RetryLimit:           10,
			RetryInterval:        1 * time.Second,
			GC: &GCConfig{
				PeerGCInterval: 1 * time.Minute,
				PeerTTL:        5 * time.Minute,
				TaskGCInterval: 1 * time.Minute,
				TaskTTL:        10 * time.Minute,
				HostGCInterval: 1 * time.Minute,
				HostTTL:        10 * time.Minute,
			},
		},
		Server: &ServerConfig{
			IP:       "127.0.0.1",
			Host:     "foo",
			Listen:   "0.0.0.0",
			Port:     8002,
			CacheDir: "foo",
			LogDir:   "bar",
		},
		DynConfig: &DynConfig{
			RefreshInterval: 5 * time.Minute,
		},
		Manager: &ManagerConfig{
			Addr:               "127.0.0.1:65003",
			SchedulerClusterID: 1,
			KeepAlive: KeepAliveConfig{
				Interval: 5 * time.Second,
			},
		},
		SeedPeer: &SeedPeerConfig{
			Enable: true,
		},
		Host: &HostConfig{
			IDC:         "foo",
			NetTopology: "bar",
			Location:    "baz",
		},
		Job: &JobConfig{
			Enable:             true,
			GlobalWorkerNum:    1,
			SchedulerWorkerNum: 1,
			LocalWorkerNum:     5,
			Redis: &RedisConfig{
				Host:      "127.0.0.1",
				Port:      6379,
				Password:  "foo",
				BrokerDB:  1,
				BackendDB: 2,
			},
		},
		Storage: &StorageConfig{
			MaxSize:    1,
			MaxBackups: 1,
			BufferSize: 1,
		},
		Metrics: &MetricsConfig{
			Enable:         false,
			Addr:           ":8000",
			EnablePeerHost: false,
		},
	}

	schedulerConfigYAML := &Config{}
	contentYAML, _ := os.ReadFile("./testdata/scheduler.yaml")
	var dataYAML map[string]any
	if err := yaml.Unmarshal(contentYAML, &dataYAML); err != nil {
		t.Fatal(err)
	}

	if err := mapstructure.Decode(dataYAML, &schedulerConfigYAML); err != nil {
		t.Fatal(err)
	}

	assert.EqualValues(config, schedulerConfigYAML)
}

func TestConfig_New(t *testing.T) {
	assert := testifyassert.New(t)
	config := New()

	assert.EqualValues(config, &Config{
		Server: &ServerConfig{
			IP:     ip.IPv4,
			Host:   fqdn.FQDNHostname,
			Listen: "0.0.0.0",
			Port:   8002,
		},
		Scheduler: &SchedulerConfig{
			Algorithm:            "default",
			BackSourceCount:      3,
			RetryBackSourceLimit: 5,
			RetryLimit:           10,
			RetryInterval:        50 * time.Millisecond,
			GC: &GCConfig{
				PeerGCInterval: 10 * time.Minute,
				PeerTTL:        24 * time.Hour,
				TaskGCInterval: 10 * time.Minute,
				TaskTTL:        24 * time.Hour,
				HostGCInterval: 30 * time.Minute,
				HostTTL:        48 * time.Hour,
			},
		},
		DynConfig: &DynConfig{
			RefreshInterval: 10 * time.Second,
		},
		Host: &HostConfig{},
		Manager: &ManagerConfig{
			SchedulerClusterID: 1,
			KeepAlive: KeepAliveConfig{
				Interval: 5 * time.Second,
			},
		},
		SeedPeer: &SeedPeerConfig{
			Enable: true,
		},
		Job: &JobConfig{
			Enable:             true,
			GlobalWorkerNum:    10,
			SchedulerWorkerNum: 10,
			LocalWorkerNum:     10,
			Redis: &RedisConfig{
				Port:      6379,
				BrokerDB:  1,
				BackendDB: 2,
			},
		},
		Storage: &StorageConfig{
			MaxSize:    storage.DefaultMaxSize,
			MaxBackups: storage.DefaultMaxBackups,
			BufferSize: storage.DefaultBufferSize,
		},
		Metrics: &MetricsConfig{
			Enable:         false,
			EnablePeerHost: false,
		},
	})
}
