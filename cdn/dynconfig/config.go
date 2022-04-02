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

package dynconfig

import (
	"fmt"
	"path/filepath"
	"time"

	dc "d7y.io/dragonfly/v2/internal/dynconfig"
	"d7y.io/dragonfly/v2/pkg/dfpath"
)

type Config struct {
	// RefreshInterval is refresh interval for manager cache.
	RefreshInterval time.Duration `yaml:"refreshInterval" mapstructure:"refreshInterval"`

	// CachePath is cache file path.
	CachePath string `yaml:"cachePath" mapstructure:"cachePath"`

	// sourceType is source type of dynConfig,
	SourceType dc.SourceType `yaml:"sourceType" mapstructure:"sourceType"`

	//// ConfigPath is dynamic config path when sourceType is local
	//ConfigPath string `yaml:"configPath" mapstructure:"configPath"`
}

func (c Config) Validate() []error {
	var errors []error
	if c.SourceType != dc.ManagerSourceType && c.SourceType != dc.LocalSourceType {
		errors = append(errors, fmt.Errorf("dynamic config sourceType only support manager or local but current is %s",
			c.SourceType))
	}
	//if c.SourceType == dc.LocalSourceType && c.ConfigPath == "" {
	//	errors = append(errors, fmt.Errorf("dynamic config sourceType is local but dynamic config path is empty"))
	//}
	if c.CachePath == "" {
		errors = append(errors, fmt.Errorf("dynamic config cache path can't be empty"))
	}
	if c.RefreshInterval <= 0 {
		errors = append(errors, fmt.Errorf("dynamic config refresh interval %d can't be a negative number", c.RefreshInterval))
	}
	return errors
}

func DefaultConfig() Config {
	config := Config{}
	return config.applyDefaults()
}

func (c Config) applyDefaults() Config {
	if c.RefreshInterval <= 0 {
		c.RefreshInterval = time.Minute
	}
	if c.CachePath == "" {
		c.CachePath = filepath.Join(dfpath.DefaultCacheDir, "cdn_dynconfig")
	}
	if c.SourceType == "" {
		c.SourceType = dc.ManagerSourceType
	}
	return c
}
