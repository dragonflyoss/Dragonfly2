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
	"d7y.io/dragonfly/v2/pkg/unit"
)

func getDefaultDriverGCConfigs(diskDriver storedriver.Driver, memoryDriver storedriver.Driver) (map[string]*storage.DriverGCConfig, error) {
	diskGCConfig, err := getDiskGCConfig(diskDriver)
	if err != nil {
		return nil, err
	}
	memoryGCConfig, err := getMemoryGCConfig(memoryDriver)
	if err != nil {
		return nil, err
	}
	return map[string]*storage.DriverGCConfig{
		"disk":   diskGCConfig,
		"memory": memoryGCConfig,
	}, nil
}

func getDiskGCConfig(diskDriver storedriver.Driver) (*storage.DriverGCConfig, error) {
	totalSpace, err := diskDriver.GetTotalSpace()
	if err != nil {
		return nil, err
	}
	yongGCThreshold := 200 * unit.GB
	if totalSpace > 0 && totalSpace/4 < yongGCThreshold {
		yongGCThreshold = totalSpace / 4
	}
	return &storage.DriverGCConfig{
		YoungGCThreshold:  yongGCThreshold,
		FullGCThreshold:   25 * unit.GB,
		IntervalThreshold: 2 * time.Hour,
		CleanRatio:        1,
	}, nil
}

func getMemoryGCConfig(memoryDriver storedriver.Driver) (*storage.DriverGCConfig, error) {
	// determine whether the shared cache can be used
	diff := unit.Bytes(0)
	totalSpace, err := memoryDriver.GetTotalSpace()
	if err != nil {
		return nil, err
	}
	if totalSpace < 72*unit.GB {
		diff = 72*unit.GB - totalSpace
	}
	return &storage.DriverGCConfig{
		YoungGCThreshold:  10*unit.GB + diff,
		FullGCThreshold:   2*unit.GB + diff,
		CleanRatio:        3,
		IntervalThreshold: 2 * time.Hour,
	}, nil
}
