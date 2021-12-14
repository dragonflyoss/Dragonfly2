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

package storage

import (
	"fmt"
	"path/filepath"
	"runtime"
	"time"

	"d7y.io/dragonfly/v2/pkg/basic"
	"d7y.io/dragonfly/v2/pkg/unit"
)

type Config struct {
	StorageMode    string                   `yaml:"storageMode"`
	GCInitialDelay time.Duration            `yaml:"gcInitialDelay"`
	GCInterval     time.Duration            `yaml:"gcInterval"`
	DriverConfigs  map[string]*DriverConfig `yaml:"driverGCConfigs"`
}

func DefaultConfig() Config {
	config := Config{}
	return config.applyDefault()
}

func (c Config) applyDefault() Config {
	if c.StorageMode == "" {
		c.StorageMode = DefaultStorageMode
	}
	if c.GCInitialDelay == 0 {
		c.GCInitialDelay = DefaultGCInitialDelay
	}
	if c.GCInterval == 0 {
		c.GCInterval = DefaultInterval
	}
	if len(c.DriverConfigs) == 0 {
		builder := Get(c.StorageMode)
		if builder != nil {
			c.DriverConfigs = builder.DefaultDriverConfigs()
		}
	}
	return c
}

func (c Config) Validate() []error {
	var errors []error
	if ok := IsSupport(c.StorageMode); !ok {
		errors = append(errors, fmt.Errorf("os %s is not support storage mode %s", runtime.GOOS, c.StorageMode))
	}
	if c.GCInitialDelay < 0 {
		errors = append(errors, fmt.Errorf("storage GCInitialDelay %d can't be a negative number", c.GCInitialDelay))
	}
	if c.GCInterval <= 0 {
		errors = append(errors, fmt.Errorf("storage GCMetaInterval must be greater than 0, but is: %d", c.GCInterval))
	}
	if len(c.DriverConfigs) == 0 {
		errors = append(errors, fmt.Errorf("storage DriverConfigs can not be empty"))
	}
	builder := Get(c.StorageMode)
	if builder == nil {
		var storageModes []string
		for s := range managerMap {
			storageModes = append(storageModes, s)
		}
		errors = append(errors, fmt.Errorf("storage StorageMode must in [%s]. but is: %s", storageModes, c.StorageMode))
	}
	errors = append(errors, builder.Validate(c.DriverConfigs)...)
	return errors
}

type DriverConfig struct {
	BaseDir        string          `yaml:"baseDir"`
	DriverGCConfig *DriverGCConfig `yaml:"driverGCConfig"`
}

// DriverGCConfig driver gc config
type DriverGCConfig struct {
	YoungGCThreshold  unit.Bytes    `yaml:"youngGCThreshold"`
	FullGCThreshold   unit.Bytes    `yaml:"fullGCThreshold"`
	CleanRatio        int           `yaml:"cleanRatio"`
	IntervalThreshold time.Duration `yaml:"intervalThreshold"`
}

const (
	// DefaultStorageMode is the default storage mode of CDN.
	DefaultStorageMode = "disk"
)

const (
	// DefaultGCInitialDelay is the delay time from the start to the first GC execution.
	DefaultGCInitialDelay = 0 * time.Second

	// DefaultInterval is the interval time to execute the GC storage.
	DefaultInterval = 15 * time.Second
)

var (
	DefaultDiskBaseDir = filepath.Join(basic.HomeDir, "ftp")

	DefaultMemoryBaseDir = "/dev/shm/dragonfly"
)
