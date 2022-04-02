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

	"d7y.io/dragonfly/v2/cdn/dynconfig"
	"d7y.io/dragonfly/v2/cdn/metrics"
	"d7y.io/dragonfly/v2/cdn/plugins"
	"d7y.io/dragonfly/v2/cdn/rpcserver"
	"d7y.io/dragonfly/v2/cdn/storedriver"
	"d7y.io/dragonfly/v2/cdn/supervisor/cdn"
	"d7y.io/dragonfly/v2/cdn/supervisor/cdn/storage"
	"d7y.io/dragonfly/v2/cdn/supervisor/task"
	"d7y.io/dragonfly/v2/cmd/dependency/base"
	dc "d7y.io/dragonfly/v2/internal/dynconfig"
	"d7y.io/dragonfly/v2/pkg/basic"
	"d7y.io/dragonfly/v2/pkg/dfpath"
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
			ListenPort:              8006,
			DownloadPort:            8000,
			SystemReservedBandwidth: 200 * unit.MB,
			MaxBandwidth:            20 * unit.GB,
			AdvertiseIP:             "127.0.0.1",
			FailAccessInterval:      30 * time.Minute,
			GCInitialDelay:          60 * time.Second,
			GCMetaInterval:          20 * time.Minute,
			TaskExpireTime:          40 * time.Minute,
			StorageMode:             "disk",
			LogDir:                  "aaa",
			WorkHome:                "/workHome",
			Manager: ManagerConfig{
				Addr:         "127.0.0.1:8004",
				CDNClusterID: 5,
				KeepAlive: KeepAliveConfig{
					Interval: 50 * time.Second,
				},
			},
			Host: HostConfig{
				Location: "beijing",
				IDC:      "na61",
			},
			Metrics: &RestConfig{
				Addr: ":8081",
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
						GCInitialDelay: 10 * time.Second,
						GCInterval:     150 * time.Second,
						DriverConfigs: map[string]*DriverConfig{
							"disk": {
								GCConfig: &GCConfig{
									YoungGCThreshold:  1000 * unit.GB,
									FullGCThreshold:   50 * unit.GB,
									CleanRatio:        3,
									IntervalThreshold: 3 * time.Hour,
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
			Addr: ":8081",
		},
		Storage: storage.Config{
			StorageMode:    "disk",
			GCInitialDelay: 10 * time.Second,
			GCInterval:     150 * time.Second,
			DriverConfigs: map[string]*storage.DriverConfig{
				"disk": {
					BaseDir: filepath.Join(basic.HomeDir, "ftp"),
					DriverGCConfig: &storage.DriverGCConfig{
						YoungGCThreshold:  1000 * unit.GB,
						FullGCThreshold:   50 * unit.GB,
						CleanRatio:        3,
						IntervalThreshold: 3 * time.Hour,
					},
				},
			},
		},
		RPCServer: rpcserver.Config{
			AdvertiseIP:  "127.0.0.1",
			Listen:       "0.0.0.0",
			ListenPort:   8006,
			DownloadPort: 8000,
		},
		Task: task.Config{
			FailAccessInterval: 30 * time.Minute,
			GCInitialDelay:     60 * time.Second,
			GCMetaInterval:     20 * time.Minute,
			ExpireTime:         40 * time.Minute,
		},
		CDN: cdn.Config{
			SystemReservedBandwidth: 200 * unit.MB,
			MaxBandwidth:            20 * unit.GB,
			WriterRoutineLimit:      4,
		},
		Manager: ManagerConfig{
			Addr:         "127.0.0.1:8004",
			CDNClusterID: 5,
			KeepAlive: KeepAliveConfig{
				Interval: 50 * time.Second,
			},
		},
		Host: HostConfig{
			Location: "beijing",
			IDC:      "na61",
		},
		LogDir:   "aaa",
		WorkHome: "/workHome",
		DynConfig: dynconfig.Config{
			RefreshInterval: time.Minute,
			CachePath:       dfpath.DefaultCacheDir,
			SourceType:      dc.ManagerSourceType,
		},
	}, cfg)
}
