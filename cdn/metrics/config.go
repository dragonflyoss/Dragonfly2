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

package metrics

import (
	"fmt"
	"net"
)

const DefaultNetwork = "tcp"

type Config struct {
	Net  string `yaml:"net" mapstructure:"net"`
	Addr string `yaml:"addr" mapstructure:"addr"`
}

func DefaultConfig() Config {
	config := Config{}
	return config.applyDefaults()
}

func (c Config) applyDefaults() Config {
	if c.Net == "" {
		c.Net = DefaultNetwork
	}
	if c.Addr == "" {
		c.Addr = ":8000"
	}
	return c
}

func (c Config) Validate() []error {
	var errors []error
	if c.Net != "" && c.Net != DefaultNetwork && c.Net != "unix" {
		errors = append(errors, fmt.Errorf("metrics net only support tcp and unix, but is: %s", c.Net))
	}
	if c.Net == DefaultNetwork {
		if _, err := net.ResolveTCPAddr(c.Net, c.Addr); err != nil {
			errors = append(errors, fmt.Errorf("metrics addr is configured incorrect: %v", err))
		}
	}
	if c.Net == "unix" {
		if _, err := net.ResolveUnixAddr(c.Net, c.Addr); err != nil {
			errors = append(errors, fmt.Errorf("metrics addr is configured incorrect: %v", err))
		}
	}
	return errors
}
