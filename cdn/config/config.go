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
	"d7y.io/dragonfly/v2/pkg/util/hostutils"
	"fmt"
	"time"

	"gopkg.in/yaml.v3"

	"d7y.io/dragonfly/v2/cdn/metrics"
	"d7y.io/dragonfly/v2/cdn/rpcserver"
	"d7y.io/dragonfly/v2/cdn/supervisor/cdn"
	"d7y.io/dragonfly/v2/cdn/supervisor/cdn/storage"
	"d7y.io/dragonfly/v2/cdn/supervisor/task"
	"d7y.io/dragonfly/v2/cmd/dependency/base"
)

// New creates an instant with default values.
func New() *Config {
	return &Config{
		Metrics:   metrics.DefaultConfig(),
		Storage:   storage.DefaultConfig(),
		RPCServer: rpcserver.DefaultConfig(),
		Task:      task.DefaultConfig(),
		CDN:       cdn.DefaultConfig(),
		Manager: ManagerConfig{
			Addr:         "",
			CDNClusterID: 0,
			KeepAlive: KeepAliveConfig{
				Interval: 5 * time.Second,
			},
		},
		Host: HostConfig{
			Location: "",
			IDC:      "",
			Hostname: hostutils.FQDNHostname,
		},
		LogDir: "",
	}
}

// Config contains all configuration of cdn node.
type Config struct {
	base.Options `yaml:",inline" mapstructure:",squash"`
	Metrics      metrics.Config   `yaml:"metrics" mapstructure:"metrics"`
	Storage      storage.Config   `yaml:"storage" mapstructure:"storage"`
	RPCServer    rpcserver.Config `yaml:"rpcServer" mapstructure:"rpcServer"`
	Task         task.Config      `yaml:"taskConfig" mapstructure:"taskConfig"`
	CDN          cdn.Config       `yaml:"cdnConfig" mapstructure:"cdnConfig"`
	// Manager configuration
	Manager ManagerConfig `yaml:"manager" mapstructure:"manager"`
	// Host configuration
	Host HostConfig `yaml:"host" mapstructure:"host"`
	// Log directory
	LogDir string `yaml:"logDir" mapstructure:"logDir"`
	// WorkHome directory
	WorkHome string `mapstructure:"workHome" yaml:"workHome"`
}

func (c *Config) String() string {
	if out, err := yaml.Marshal(c); err == nil {
		return string(out)
	}
	return ""
}

func (c *Config) Validate() []error {
	var errs []error
	errs = append(errs, c.Metrics.Validate()...)
	errs = append(errs, c.Storage.Validate()...)
	errs = append(errs, c.RPCServer.Validate()...)
	errs = append(errs, c.Task.Validate()...)
	errs = append(errs, c.CDN.Validate()...)
	errs = append(errs, c.Manager.Validate()...)
	return errs
}

type ManagerConfig struct {
	// NetAddr is manager address.
	Addr string `yaml:"addr" mapstructure:"addr"`

	// CDNClusterID is cdn cluster id.
	CDNClusterID uint `yaml:"cdnClusterID" mapstructure:"cdnClusterID"`

	// KeepAlive configuration
	KeepAlive KeepAliveConfig `yaml:"keepAlive" mapstructure:"keepAlive"`
}

func (c ManagerConfig) Validate() []error {
	var errors []error
	if c.Addr != "" {
		if c.CDNClusterID <= 0 {
			errors = append(errors, fmt.Errorf("cdn cluster id %d can't be a negative number", c.CDNClusterID))
		}
		errors = append(errors, c.KeepAlive.Validate()...)
	}
	return errors
}

type KeepAliveConfig struct {
	// Keep alive interval
	Interval time.Duration `yaml:"interval" mapstructure:"interval"`
}

func (c KeepAliveConfig) Validate() []error {
	var errors []error
	if c.Interval <= 0 {
		errors = append(errors, fmt.Errorf("keep alive interval %d can't be a negative number", c.Interval))
	}
	return errors
}

type HostConfig struct {
	// Location for cdn
	Location string `mapstructure:"location" yaml:"location"`

	// IDC for cdn
	IDC string `mapstructure:"idc" yaml:"idc"`

	// Hostname for cdn
	Hostname string `mapstructure:"idc" yaml:"idc"`
}
