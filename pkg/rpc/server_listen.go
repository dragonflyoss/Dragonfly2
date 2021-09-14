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

package rpc

import (
	"fmt"
	"net"

	"google.golang.org/grpc"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
)

// Listen wraps net.Listen with dfnet.NetAddr
// Example:
//    Listen(dfnet.NetAddr{Type: dfnet.UNIX, Addr: "/var/run/df.sock"})
//    Listen(dfnet.NetAddr{Type: dfnet.TCP, Addr: ":12345"})
func Listen(netAddr dfnet.NetAddr) (net.Listener, error) {
	return net.Listen(string(netAddr.Type), netAddr.Addr)
}

// ListenWithPortRange tries to listen a port between startPort and endPort, return net.Listener and listen port
// Example:
//    ListenWithPortRange("0.0.0.0", 12345, 23456)
//    ListenWithPortRange("192.168.0.1", 12345, 23456)
//    ListenWithPortRange("192.168.0.1", 0, 0) // random port
func ListenWithPortRange(listen string, startPort, endPort int) (net.Listener, int, error) {
	if endPort < startPort {
		endPort = startPort
	}
	for port := startPort; port <= endPort; port++ {
		logger.Debugf("start to listen port: %s:%d", listen, port)
		listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", listen, port))
		if err == nil && listener != nil {
			return listener, listener.Addr().(*net.TCPAddr).Port, nil
		}
		if isErrAddrInuse(err) {
			logger.Warnf("listen port %s:%d is in used, sys error: %s", listen, port, err)
			continue
		} else if err != nil {
			logger.Warnf("listen port %s:%d error: %s", listen, port, err)
			return nil, -1, err
		}
	}
	return nil, -1, fmt.Errorf("no available port to listen, port: %d - %d", startPort, endPort)
}

type Server interface {
	Serve(net.Listener) error
	Stop()
	GracefulStop()
}

// NewServer returns a Server with impl
// Example:
//    s := NewServer(impl, ...)
//    s.Serve(listener)
func NewServer(impl interface{}, opts ...grpc.ServerOption) Server {
	server := grpc.NewServer(append(serverOpts, opts...)...)
	register(server, impl)
	return server
}
