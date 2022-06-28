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

	"gopkg.in/yaml.v3"
)

type NetworkType string

const (
	TCP   NetworkType = "tcp"
	UNIX  NetworkType = "unix"
	VSOCK NetworkType = "vsock"

	TCPEndpointPrefix   string = "dns:///"
	UnixEndpointPrefix  string = "unix://"
	VsockEndpointPrefix string = "vsock://"
)

type NetAddr struct {
	Type NetworkType `mapstructure:"type" yaml:"type"`
	// see https://github.com/grpc/grpc/blob/master/doc/naming.md
	Addr string `mapstructure:"addr" yaml:"addr"`
}

func (n NetAddr) GetEndpoint() string {
	switch n.Type {
	case UNIX:
		return UnixEndpointPrefix + n.Addr
	case VSOCK:
		return VsockEndpointPrefix + n.Addr
	default:
		return TCPEndpointPrefix + n.Addr
	}
}

func (n NetAddr) String() string {
	return n.GetEndpoint()
}

func (n *NetAddr) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}

	switch value := v.(type) {
	case string:
		n.Type = TCP
		n.Addr = value
		return nil
	case map[string]interface{}:
		if err := n.unmarshal(json.Unmarshal, b); err != nil {
			return err
		}
		return nil
	default:
		return errors.New("invalid net addr")
	}
}

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
		var m = make(map[string]interface{})
		for i := 0; i < len(node.Content); i += 2 {
			var (
				key   string
				value interface{}
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

func (n *NetAddr) unmarshal(unmarshal func(in []byte, out interface{}) (err error), b []byte) error {
	nt := struct {
		Type NetworkType `json:"type" yaml:"type"`
		Addr string      `json:"addr" yaml:"addr"` // see https://github.com/grpc/grpc/blob/master/doc/naming.md
	}{}

	if err := unmarshal(b, &nt); err != nil {
		return err
	}

	n.Type = nt.Type
	n.Addr = nt.Addr

	return nil
}

func Convert2NetAddr(addrs []string) []NetAddr {
	netAddrs := make([]NetAddr, 0, len(addrs))
	for i := range addrs {
		netAddrs = append(netAddrs, NetAddr{
			Type: TCP,
			Addr: addrs[i],
		})
	}
	return netAddrs
}
