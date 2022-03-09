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
	"fmt"
	"time"

	"d7y.io/dragonfly/v2/cdn/storedriver"
	"d7y.io/dragonfly/v2/cdn/storedriver/local"
	"d7y.io/dragonfly/v2/cdn/supervisor/cdn/storage"
	"d7y.io/dragonfly/v2/pkg/unit"
)

func getDefaultDriverGCConfigs() map[string]*storage.DriverConfig {
	diskDriver, err := local.NewStorageDriver(&storedriver.Config{BaseDir: storage.DefaultDiskBaseDir})
	if err != nil {
		return nil
	}
	memoryDriver, err := local.NewStorageDriver(&storedriver.Config{BaseDir: storage.DefaultMemoryBaseDir})
	if err != nil {
		return nil
	}
	diskDriverConfig, err := getDiskDriverConfig(diskDriver)
	if err != nil {
		return nil
	}
	memoryDriverConfig, err := getMemoryDriverConfig(memoryDriver)
	if err != nil {
		return nil
	}
	return map[string]*storage.DriverConfig{
		local.DiskDriverName:   diskDriverConfig,
		local.MemoryDriverName: memoryDriverConfig,
	}
}

func validateConfig(driverConfigs map[string]*storage.DriverConfig) []error {
	var errs []error
	if len(driverConfigs) != 2 || driverConfigs[local.DiskDriverName] == nil || driverConfigs[local.MemoryDriverName] == nil {
		errs = append(errs, fmt.Errorf("hybrid storage manager should have both disk and memory driver, but is: %v", driverConfigs))
	}
	return errs
}

func getDiskDriverConfig(diskDriver storedriver.Driver) (*storage.DriverConfig, error) {
	totalSpace, err := diskDriver.GetTotalSpace()
	if err != nil {
		return nil, err
	}
	yongGCThreshold := 200 * unit.GB
	if totalSpace > 0 && totalSpace/4 < yongGCThreshold {
		yongGCThreshold = totalSpace / 4
	}
	return &storage.DriverConfig{
		BaseDir: diskDriver.GetBaseDir(),
		DriverGCConfig: &storage.DriverGCConfig{
			YoungGCThreshold:  yongGCThreshold,
			FullGCThreshold:   5 * unit.GB,
			IntervalThreshold: 2 * time.Hour,
			CleanRatio:        1,
		},
	}, nil
}

func getMemoryDriverConfig(memoryDriver storedriver.Driver) (*storage.DriverConfig, error) {
	// determine whether the shared cache can be used
	diff := unit.Bytes(0)
	totalSpace, err := memoryDriver.GetTotalSpace()
	if err != nil {
		return nil, err
	}
	if totalSpace < 72*unit.GB {
		diff = 72*unit.GB - totalSpace
	}
	return &storage.DriverConfig{
		BaseDir: memoryDriver.GetBaseDir(),
		DriverGCConfig: &storage.DriverGCConfig{
			YoungGCThreshold:  10*unit.GB + diff,
			FullGCThreshold:   2*unit.GB + diff,
			CleanRatio:        3,
			IntervalThreshold: 2 * time.Hour,
		},
	}, nil
}
