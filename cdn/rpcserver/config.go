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

import "d7y.io/dragonfly/v2/pkg/util/net/iputils"

type Config struct {

	// AdvertiseIP is used to set the ip that we advertise to other peer in the p2p-network.
	// By default, the first non-loop address is advertised.
	AdvertiseIP string `yaml:"advertiseIP" mapstructure:"advertiseIP"`

	// ListenPort is the port cdn server listens on.
	// default: 8002
	ListenPort int `yaml:"listenPort" mapstructure:"listenPort"`

	// DownloadPort is the port for download files from cdn.
	// default: 8001
	DownloadPort int `yaml:"downloadPort" mapstructure:"downloadPort"`
}

func (config Config) applyDefaults() Config {
	if config.AdvertiseIP == "" {
		config.AdvertiseIP = iputils.IPv4
	}
	if config.ListenPort == 0 {
		config.ListenPort = DefaultListenPort
	}
	if config.DownloadPort == 0 {
		config.DownloadPort = DefaultDownloadPort
	}
	return config
}

const (
	// DefaultListenPort is the default port cdn server listens on.
	DefaultListenPort = 8003
	// DefaultDownloadPort is the default port for download files from cdn.
	DefaultDownloadPort = 8001
)
