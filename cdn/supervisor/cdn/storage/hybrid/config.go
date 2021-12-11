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

package hybrid

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
	MemoryGCConfig storage.GCConfig `yaml:"memoryGCConfig"`
	DiskGCConfig   storage.GCConfig `yaml:"diskGCConfig"`
}

func applyDefaults(diskDriver storedriver.Driver, memoryDriver storedriver.Driver, storageConfig storage.Config) Config {
	cfg := Config{}
	if storageConfig.GCInitialDelay == 0 {
		cfg.GCInitialDelay = 0 * time.Second
	}
	if storageConfig.GCInterval == 0 {
		cfg.GCInterval = 15 * time.Second
	}
	cfg.DiskGCConfig = getDiskGCConfig(diskDriver, storageConfig.DriverConfigs["disk"])
	cfg.MemoryGCConfig = getMemoryGCConfig(memoryDriver, storageConfig.DriverConfigs["memory"])
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

func getMemoryGCConfig(memoryDriver storedriver.Driver, config *storage.DriverConfig) storage.GCConfig {
	// determine whether the shared cache can be used
	if config != nil && config.GCConfig != nil {
		return *config.GCConfig
	}
	diff := unit.Bytes(0)
	totalSpace, err := memoryDriver.GetTotalSpace()
	if err != nil {
		logger.GcLogger.With("type", "hybrid").Errorf("failed to get total space of memory: %v", err)
	}
	if totalSpace < 72*unit.GB {
		diff = 72*unit.GB - totalSpace
	}
	return storage.GCConfig{
		YoungGCThreshold:  10*unit.GB + diff,
		FullGCThreshold:   2*unit.GB + diff,
		CleanRatio:        3,
		IntervalThreshold: 2 * time.Hour,
	}
}
