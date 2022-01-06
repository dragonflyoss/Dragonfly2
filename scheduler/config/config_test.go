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
		Server: &ServerConfig{
			IP:          "127.0.0.1",
			Host:        "foo",
			Port:        8002,
			ListenLimit: 1000,
			CacheDir:    "foo",
			LogDir:      "bar",
		},
		Scheduler: &SchedulerConfig{
			Algorithm:       "default",
			BackSourceCount: 3,
			RetryLimit:      10,
			RetryInterval:   1 * time.Second,
			GC: &GCConfig{
				PeerGCInterval: 1 * time.Minute,
				PeerTTL:        5 * time.Minute,
				TaskGCInterval: 1 * time.Minute,
				TaskTTL:        10 * time.Minute,
			},
		},
		DynConfig: &DynConfig{
			RefreshInterval: 5 * time.Minute,
			CDNDir:          "foo",
		},
		Host: &HostConfig{
			IDC:         "foo",
			NetTopology: "bar",
			Location:    "baz",
		},
		Manager: &ManagerConfig{
			Enable:             true,
			Addr:               "127.0.0.1:65003",
			SchedulerClusterID: 1,
			KeepAlive: KeepAliveConfig{
				Interval: 5 * time.Second,
			},
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
		Metrics: &MetricsConfig{
			Enable:         false,
			Addr:           ":8000",
			EnablePeerHost: false,
		},
	}

	schedulerConfigYAML := &Config{}
	contentYAML, _ := os.ReadFile("./testdata/scheduler.yaml")
	var dataYAML map[string]interface{}
	if err := yaml.Unmarshal(contentYAML, &dataYAML); err != nil {
		t.Fatal(err)
	}

	if err := mapstructure.Decode(dataYAML, &schedulerConfigYAML); err != nil {
		t.Fatal(err)
	}

	assert.EqualValues(config, schedulerConfigYAML)
}
