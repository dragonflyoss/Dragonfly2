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
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/cdn/metrics"
	"d7y.io/dragonfly/v2/cdn/plugins"
	"d7y.io/dragonfly/v2/cdn/rpcserver"
	"d7y.io/dragonfly/v2/cdn/storedriver"
	"d7y.io/dragonfly/v2/cdn/supervisor/cdn"
	"d7y.io/dragonfly/v2/cdn/supervisor/cdn/storage"
	"d7y.io/dragonfly/v2/cdn/supervisor/task"
	"d7y.io/dragonfly/v2/cmd/dependency/base"
	"d7y.io/dragonfly/v2/pkg/basic"
	"d7y.io/dragonfly/v2/pkg/unit"
)

func TestConfig_Convert(t *testing.T) {
	dcfg := &DeprecatedConfig{
		Options: base.Options{
			Console:   true,
			Verbose:   true,
			PProfPort: 1000,
			Telemetry: base.TelemetryOption{
				Jaeger:      "https://jaeger.com",
				ServiceName: "dragonfly-config-test",
			},
		},
		BaseProperties: &BaseProperties{
			ListenPort:              8003,
			DownloadPort:            8001,
			SystemReservedBandwidth: 20 * unit.MB,
			MaxBandwidth:            2 * unit.GB,
			AdvertiseIP:             "127.0.0.1",
			FailAccessInterval:      3 * time.Minute,
			GCInitialDelay:          6 * time.Second,
			GCMetaInterval:          2 * time.Minute,
			TaskExpireTime:          4 * time.Minute,
			StorageMode:             "disk",
			LogDir:                  "aaa",
			WorkHome:                "/workHome",
			Manager: ManagerConfig{
				Addr:         "127.0.0.1:8004",
				CDNClusterID: 5,
				KeepAlive: KeepAliveConfig{
					Interval: 5 * time.Second,
				},
			},
			Host: HostConfig{
				Location: "beijing",
				IDC:      "na61",
			},
			Metrics: &RestConfig{
				Addr: ":8080",
			},
		},
		Plugins: map[plugins.PluginType][]*plugins.PluginProperties{
			plugins.StorageDriverPlugin: {
				{
					Name:   "disk",
					Enable: true,
					Config: &storedriver.Config{
						BaseDir: filepath.Join(basic.HomeDir, "ftp"),
					},
				},
			}, plugins.StorageManagerPlugin: {
				{
					Name:   "disk",
					Enable: true,
					Config: &StorageConfig{
						GCInitialDelay: 0 * time.Second,
						GCInterval:     15 * time.Second,
						DriverConfigs: map[string]*DriverConfig{
							"disk": {
								GCConfig: &GCConfig{
									YoungGCThreshold:  100 * unit.GB,
									FullGCThreshold:   5 * unit.GB,
									CleanRatio:        1,
									IntervalThreshold: 2 * time.Hour,
								},
							},
						},
					},
				},
			},
		},
	}
	cfg := dcfg.Convert()

	assert.EqualValues(t, &Config{
		Options: base.Options{
			Console:   true,
			Verbose:   true,
			PProfPort: 1000,
			Telemetry: base.TelemetryOption{
				Jaeger:      "https://jaeger.com",
				ServiceName: "dragonfly-config-test",
			},
		},
		Metrics: metrics.Config{
			Net:  "tcp",
			Addr: ":8080",
		},
		Storage: storage.Config{
			StorageMode:    "disk",
			GCInitialDelay: 0,
			GCInterval:     15000000000,
			DriverConfigs: map[string]*storage.DriverConfig{
				"disk": {
					BaseDir: filepath.Join(basic.HomeDir, "ftp"),
					DriverGCConfig: &storage.DriverGCConfig{
						YoungGCThreshold:  100 * unit.GB,
						FullGCThreshold:   5 * unit.GB,
						CleanRatio:        1,
						IntervalThreshold: 2 * time.Hour,
					},
				},
			},
		},
		RPCServer: rpcserver.Config{
			AdvertiseIP:  "127.0.0.1",
			ListenPort:   8003,
			DownloadPort: 8001,
		},
		Task: task.Config{
			FailAccessInterval: 3 * time.Minute,
			GCInitialDelay:     6 * time.Second,
			GCMetaInterval:     2 * time.Minute,
			ExpireTime:         4 * time.Minute,
		},
		CDN: cdn.Config{
			SystemReservedBandwidth: 20 * unit.MB,
			MaxBandwidth:            2 * unit.GB,
			WriterRoutineLimit:      4,
		},
		Manager: ManagerConfig{
			Addr:         "127.0.0.1:8004",
			CDNClusterID: 5,
			KeepAlive: KeepAliveConfig{
				Interval: 5 * time.Second,
			},
		},
		Host: HostConfig{
			Location: "beijing",
			IDC:      "na61",
		},
		LogDir:   "aaa",
		WorkHome: "/workHome",
	}, cfg)
}
