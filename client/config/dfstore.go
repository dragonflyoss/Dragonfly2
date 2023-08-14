/*
 *     Copyright 2022 The Dragonfly Authors
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
	"errors"
	"fmt"
	"net"
	"net/url"
)

type DfstoreConfig struct {
	// Address of the object storage service.
	Endpoint string `yaml:"endpoint,omitempty" mapstructure:"endpoint,omitempty"`

	// Filter is used to generate a unique Task ID by
	// filtering unnecessary query params in the URL,
	// it is separated by & character.
	Filter string `yaml:"filter,omitempty" mapstructure:"filter,omitempty"`

	// Mode is the mode in which the backend is written,
	// including WriteBack and AsyncWriteBack.
	Mode int `yaml:"mode,omitempty" mapstructure:"mode,omitempty"`

	// MaxReplicas is the maximum number of
	// replicas of an object cache in seed peers.
	MaxReplicas int `yaml:"maxReplicas,omitempty" mapstructure:"mode,maxReplicas"`
}

// New dfstore configuration.
func NewDfstore() *DfstoreConfig {
	url := url.URL{
		Scheme: "http",
		Host:   net.JoinHostPort("127.0.0.1", fmt.Sprint(DefaultObjectStorageStartPort)),
	}

	return &DfstoreConfig{
		Endpoint:    url.String(),
		MaxReplicas: DefaultObjectMaxReplicas,
	}
}

func (cfg *DfstoreConfig) Validate() error {
	if cfg.Endpoint == "" {
		return errors.New("dfstore requires parameter endpoint")
	}

	if _, err := url.ParseRequestURI(cfg.Endpoint); err != nil {
		return fmt.Errorf("invalid endpoint: %w", err)
	}

	return nil
}
