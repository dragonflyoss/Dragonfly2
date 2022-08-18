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

package dfnet

import (
	"encoding/json"
	"errors"
	"fmt"

	"gopkg.in/yaml.v3"
)

type NetworkType string

const (
	// TCP represents protocol of tcp.
	TCP NetworkType = "tcp"

	// TCP represents protocol of unix.
	UNIX NetworkType = "unix"

	// TCP represents protocol of vsock.
	VSOCK NetworkType = "vsock"
)

// NetAddr is the definition structure of grpc address,
// refer to https://github.com/grpc/grpc/blob/master/doc/naming.md.
type NetAddr struct {
	// Type is the type of network.
	Type NetworkType `mapstructure:"type" yaml:"type"`

	// Addr is the address of network.
	Addr string `mapstructure:"addr" yaml:"addr"`
}

// String returns the endpoint of network address.
func (n *NetAddr) String() string {
	switch n.Type {
	case UNIX:
		return fmt.Sprintf("unix://%s", n.Addr)
	case VSOCK:
		return fmt.Sprintf("vsock://%s", n.Addr)
	default:
		return fmt.Sprintf("dns:///%s", n.Addr)
	}
}

// UnmarshalJSON parses the JSON-encoded data and stores the result in NetAddr.
func (n *NetAddr) UnmarshalJSON(b []byte) error {
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}

	switch value := v.(type) {
	case string:
		n.Type = TCP
		n.Addr = value
		return nil
	case map[string]any:
		if err := n.unmarshal(json.Unmarshal, b); err != nil {
			return err
		}
		return nil
	default:
		return errors.New("invalid net addr")
	}
}

// UnmarshalYAML parses the YAML-encoded data and stores the result in NetAddr.
func (n *NetAddr) UnmarshalYAML(node *yaml.Node) error {
	switch node.Kind {
	case yaml.ScalarNode:
		var addr string
		if err := node.Decode(&addr); err != nil {
			return err
		}

		n.Type = TCP
		n.Addr = addr
		return nil
	case yaml.MappingNode:
		var m = make(map[string]any)
		for i := 0; i < len(node.Content); i += 2 {
			var (
				key   string
				value any
			)
			if err := node.Content[i].Decode(&key); err != nil {
				return err
			}

			if err := node.Content[i+1].Decode(&value); err != nil {
				return err
			}

			m[key] = value
		}

		b, err := yaml.Marshal(m)
		if err != nil {
			return err
		}

		if err := n.unmarshal(yaml.Unmarshal, b); err != nil {
			return err
		}

		return nil
	default:
		return errors.New("invalid net addr")
	}
}

// unmarshal parses the encoded data and stores the result.
func (n *NetAddr) unmarshal(unmarshal func(in []byte, out any) (err error), b []byte) error {
	netAddr := struct {
		Type NetworkType `json:"type" yaml:"type"`
		Addr string      `json:"addr" yaml:"addr"`
	}{}

	if err := unmarshal(b, &netAddr); err != nil {
		return err
	}

	n.Type = netAddr.Type
	n.Addr = netAddr.Addr
	return nil
}
