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
			Training: &TrainingConfig{
				Enable:               true,
				EnableAutoRefresh:    true,
				RefreshModelInterval: 1 * time.Second,
				CPU:                  2,
			},
		},
		Server: &ServerConfig{
			AdvertiseIP: "127.0.0.1",
			ListenIP:    "0.0.0.0",
			Port:        8002,
			Host:        "foo",
			WorkHome:    "home",
			CacheDir:    "foo",
			LogDir:      "bar",
			DataDir:     "baz",
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
				Addrs:     []string{"foo", "bar"},
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
		Security: &SecurityConfig{
			AutoIssueCert: true,
			CACert:        "foo",
			TLSVerify:     true,
			TLSPolicy:     "force",
			CertSpec: &CertSpec{
				ValidityPeriod: 1000,
			},
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
