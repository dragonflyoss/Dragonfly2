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
	"io/ioutil"
	"testing"
	"time"

	dc "d7y.io/dragonfly.v2/internal/dynconfig"
	"github.com/mitchellh/mapstructure"
	testifyassert "github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

func TestSchedulerConfig_Load(t *testing.T) {
	assert := testifyassert.New(t)

	config := &Config{
		Manager: ManagerConfig{
			Addr:               "127.0.0.1:65003",
			SchedulerClusterID: 1,
			KeepAlive: KeepAliveConfig{
				Interval:         1 * time.Second,
				RetryMaxAttempts: 100,
				RetryInitBackOff: 100,
				RetryMaxBackOff:  100,
			},
		},
		Dynconfig: &DynconfigOptions{
			Type:       dc.LocalSourceType,
			Path:       "foo",
			CachePath:  "bar",
			ExpireTime: 1000,
			Addr:       "127.0.0.1:8002",
			CDNDirPath: "tmp",
		},
		Scheduler: SchedulerConfig{
			ABTest:     true,
			AScheduler: "a-scheduler",
			BScheduler: "b-scheduler",
		},
		Server: ServerConfig{
			IP:   "127.0.0.1",
			Port: 8002,
		},
		Worker: SchedulerWorkerConfig{
			WorkerNum:         8,
			WorkerJobPoolSize: 10000,
			SenderNum:         10,
			SenderJobPoolSize: 10000,
		},
		GC: GCConfig{
			TaskDelay:     3600 * 1000,
			PeerTaskDelay: 3600 * 1000,
		},
	}

	schedulerConfigYAML := &Config{}
	contentYAML, _ := ioutil.ReadFile("./testdata/scheduler.yaml")
	var dataYAML map[string]interface{}
	yaml.Unmarshal(contentYAML, &dataYAML)
	mapstructure.Decode(dataYAML, &schedulerConfigYAML)
	assert.EqualValues(config, schedulerConfigYAML)
}
