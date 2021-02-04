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
	"fmt"
	"net"
	"os"

	"d7y.io/dragonfly/v2/pkg/util/jsonutils"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
)

type NetworkType string

func (n *NetworkType) UnmarshalJSON(b []byte) error {
	var t string
	err := json.Unmarshal(b, &t)
	if err != nil {
		return err
	}

	*n = NetworkType(t)
	return nil
}

func (n *NetworkType) UnmarshalYAML(node *yaml.Node) error {
	var t string
	switch node.Kind {
	case yaml.ScalarNode:
		if err := node.Decode(&t); err != nil {
			return err
		}
	default:
		return errors.New("invalid filestring")
	}

	*n = NetworkType(t)
	return nil
}

const (
	TCP  NetworkType = "tcp"
	UNIX NetworkType = "unix"
)

var HostIp string
var HostName, _ = os.Hostname()

func init() {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		panic(fmt.Sprintf("%v", err))
	}

	for _, value := range addrs {
		if ipNet, ok := value.(*net.IPNet); ok &&
			!ipNet.IP.IsLoopback() && !ipNet.IP.IsUnspecified() {
			if ip := ipNet.IP.To4(); ip != nil {
				HostIp = ip.String()
				break
			}
		}
	}

	if HostIp == "" {
		panic("host ip is not exist")
	}

	if HostName == "" {
		panic("host name is not exist")
	}
}

type NetAddr struct {
	Type NetworkType `json:"type" yaml:"type"`
	Addr string      `json:"addr" yaml:"addr"` // see https://github.com/grpc/grpc/blob/master/doc/naming.md
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
		if err := n.unmarshal(json.Unmarshal, v.(map[string]interface{})); err != nil {
			return err
		}
		return nil
	default:
		return errors.New("invalid proxy")
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
		if err := n.unmarshal(yaml.Unmarshal, m); err != nil {
			return err
		}
		return nil
	default:
		return errors.New("invalid proxy")
	}
}

func (n *NetAddr) unmarshal(unmarshal func(in []byte, out interface{}) (err error), m map[string]interface{}) error {
	mb, err := jsonutils.MarshalMap(m)
	if err != nil {
		return err
	}

	// Type
	if mb["type"] != nil {
		var nt NetworkType
		if err := unmarshal(mb["type"], &nt); err != nil {
			return err
		}
		n.Type = nt
	}

	// Addr
	if mb["addr"] != nil {
		var a string
		if err := unmarshal(mb["addr"], &a); err != nil {
			return err
		}
		n.Addr = a
	}

	return nil
}

func (na *NetAddr) GetEndpoint() string {
	switch na.Type {
	case UNIX:
		return "unix://" + na.Addr
	default:
		return "dns:///" + na.Addr
	}
}
