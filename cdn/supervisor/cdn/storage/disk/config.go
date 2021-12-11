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

package disk

import (
	"time"

	"d7y.io/dragonfly/v2/cdn/storedriver"
	"d7y.io/dragonfly/v2/cdn/supervisor/cdn/storage"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/unit"
)

type Config struct {
	GCInitialDelay time.Duration    `yaml:"gcInitialDelay"`
	GCInterval     time.Duration    `yaml:"gcInterval"`
	GCConfig       storage.GCConfig `yaml:"gcConfig"`
}

func applyDefaults(driver storedriver.Driver, storageConfig storage.Config) Config {
	cfg := Config{
		GCInitialDelay: 0 * time.Second,
		GCInterval:     15 * time.Second,
	}
	if storageConfig.GCInitialDelay != 0 {
		cfg.GCInitialDelay = storageConfig.GCInitialDelay
	}
	if storageConfig.GCInterval != 0 {
		cfg.GCInterval = storageConfig.GCInterval
	}
	cfg.GCConfig = getDiskGCConfig(driver, storageConfig.DriverConfigs["disk"])
	return cfg
}

func getDiskGCConfig(diskDriver storedriver.Driver, config *storage.DriverConfig) storage.GCConfig {
	if config != nil && config.GCConfig != nil {
		return *config.GCConfig
	}
	totalSpace, err := diskDriver.GetTotalSpace()
	if err != nil {
		logger.GcLogger.With("type", "hybrid").Errorf("failed to get total space of disk: %v", err)
	}
	yongGCThreshold := 200 * unit.GB
	if totalSpace > 0 && totalSpace/4 < yongGCThreshold {
		yongGCThreshold = totalSpace / 4
	}
	return storage.GCConfig{
		YoungGCThreshold:  yongGCThreshold,
		FullGCThreshold:   25 * unit.GB,
		IntervalThreshold: 2 * time.Hour,
		CleanRatio:        1,
	}
}
