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

package basic

import (
	"fmt"
	"net"
	"os"
)

type NetworkType string

const (
	TCP  NetworkType = "tcp"
	UNIX NetworkType = "unix"
)

var LocalIp string
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
				LocalIp = ip.String()
				break
			}
		}
	}

	if LocalIp == "" {
		panic("host ip is not exist")
	}

	if HostName == "" {
		panic("host name is not exist")
	}
}

type NetAddr struct {
	Type NetworkType
	Addr string // see https://github.com/grpc/grpc/blob/master/doc/naming.md
}

func (na *NetAddr) GetEndpoint() string {
	switch na.Type {
	case UNIX:
		return "unix://" + na.Addr
	default:
		return "dns:///" + na.Addr
	}
}
