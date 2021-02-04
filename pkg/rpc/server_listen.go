package rpc

import (
	"fmt"
	"net"

	"google.golang.org/grpc"

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
	for ; startPort <= endPort; startPort++ {
		listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", listen, startPort))
		if err == nil && listener != nil {
			return listener, listener.Addr().(*net.TCPAddr).Port, nil
		}
		if isErrAddrInuse(err) {
			continue
		} else if err != nil {
			return nil, -1, err
		}
	}
	return nil, -1, fmt.Errorf("")
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
