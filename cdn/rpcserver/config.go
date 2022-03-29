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

package rpcserver

import (
	"fmt"

	"d7y.io/dragonfly/v2/pkg/util/net/iputils"
)

type Config struct {

	// AdvertiseIP is used to set the ip that we advertise to other peer in the p2p-network.
	// By default, the first non-loop address is advertised.
	AdvertiseIP string `yaml:"advertiseIP" mapstructure:"advertiseIP"`

	// Listen stands listen interface, like: 0.0.0.0, 192.168.0.1
	Listen string `yaml:"listen" mapstructure:"listen"`

	// ListenPort is the port cdn server listens on.
	// default: 8002
	ListenPort int `yaml:"listenPort" mapstructure:"listenPort"`

	// DownloadPort is the port for download files from cdn.
	// default: 8001
	DownloadPort int `yaml:"downloadPort" mapstructure:"downloadPort"`
}

func DefaultConfig() Config {
	config := Config{}
	return config.applyDefaults()
}

func (c Config) applyDefaults() Config {
	if c.AdvertiseIP == "" {
		c.AdvertiseIP = iputils.IPv4
	}
	if c.Listen == "" {
		c.Listen = "0.0.0.0"
	}
	if c.ListenPort == 0 {
		c.ListenPort = DefaultListenPort
	}
	if c.DownloadPort == 0 {
		c.DownloadPort = DefaultDownloadPort
	}
	return c
}

func (c Config) Validate() []error {
	var errors []error
	if c.AdvertiseIP == "" {
		errors = append(errors, fmt.Errorf("rpc server AdvertiseIP can't be empty"))
	}
	if c.ListenPort > 65535 || c.ListenPort < 1024 {
		errors = append(errors, fmt.Errorf("rpc server ListenPort must be between 0 and 65535, inclusive. but is: %d", c.ListenPort))
	}
	if c.DownloadPort > 65535 || c.DownloadPort < 1024 {
		errors = append(errors, fmt.Errorf("rpc server DownloadPort must be between 0 and 65535, inclusive. but is: %d", c.DownloadPort))
	}
	return errors
}

const (
	// DefaultListenPort is the default port cdn server listens on.
	DefaultListenPort = 8003
	// DefaultDownloadPort is the default port for download files from cdn.
	DefaultDownloadPort = 8001
)
