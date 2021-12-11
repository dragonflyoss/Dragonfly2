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

package cdn

import "d7y.io/dragonfly/v2/pkg/unit"

type Config struct {
	// SystemReservedBandwidth is the network bandwidth reserved for system software.
	// default: 20 MB, in format of G(B)/g/M(B)/m/K(B)/k/B, pure number will also be parsed as Byte.
	SystemReservedBandwidth unit.Bytes `yaml:"systemReservedBandwidth" mapstructure:"systemReservedBandwidth"`

	// MaxBandwidth is the network bandwidth that cdn system can use.
	// default: 200 MB, in format of G(B)/g/M(B)/m/K(B)/k/B, pure number will also be parsed as Byte.
	MaxBandwidth unit.Bytes `yaml:"maxBandwidth" mapstructure:"maxBandwidth"`
}

func (c Config) applyDefaults() Config {
	if c.SystemReservedBandwidth == 0 {
		c.SystemReservedBandwidth = DefaultSystemReservedBandwidth
	}
	if c.MaxBandwidth == 0 {
		c.MaxBandwidth = DefaultMaxBandwidth
	}
	return c
}

const (
	// DefaultSystemReservedBandwidth is the default network bandwidth reserved for system software.
	// unit: MB/s
	DefaultSystemReservedBandwidth = 20 * unit.MB
	// DefaultMaxBandwidth is the default network bandwidth that cdn can use.
	// unit: MB/s
	DefaultMaxBandwidth = 1 * unit.GB
)
