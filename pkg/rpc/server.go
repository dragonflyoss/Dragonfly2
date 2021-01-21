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
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	"github.com/dragonflyoss/Dragonfly2/pkg/basic/dfnet"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/fileutils"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/stringutils"
)

type RegisterFunc func(*grpc.Server, interface{})

var (
	register   RegisterFunc
	tcpServer  *grpc.Server
	unixServer *grpc.Server
	mutex      sync.Mutex
)

func SetRegister(f RegisterFunc) {
	if f == nil {
		return
	}

	mutex.Lock()
	defer mutex.Unlock()

	if register != nil {
		panic("duplicated service register")
	}

	register = f
}

var serverOpts = []grpc.ServerOption{
	grpc.ConnectionTimeout(10 * time.Second),
	grpc.InitialConnWindowSize(8 * 1024 * 1024),
	grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
		MinTime: 1 * time.Minute,
	}),
	grpc.KeepaliveParams(keepalive.ServerParameters{
		MaxConnectionIdle: 5 * time.Minute,
	}),
	grpc.MaxConcurrentStreams(100),
	grpc.StreamInterceptor(streamServerInterceptor),
	grpc.UnaryInterceptor(unaryServerInterceptor),
}

var sp = struct {
	port int
	ch   chan struct{}
	once sync.Once
}{ch: make(chan struct{})}

func GetTcpServerPort() int {
	sp.once.Do(func() {
		<-sp.ch
	})

	return sp.port
}

// for client, start tcp first and then start unix on server process
func StartTcpServer(incrementPort int, upLimit int, impl interface{}, opts ...grpc.ServerOption) {
	for {
		if incrementPort > upLimit {
			panic(errors.New("no ports available"))
		}

		netAddr := dfnet.NetAddr{
			Type: dfnet.TCP,
			Addr: fmt.Sprintf(":%d", incrementPort),
		}

		if err := startServer(netAddr, impl, opts); err != nil && !isErrAddrInuse(err) {
			panic(err)
		} else if err == nil {
			return
		}

		incrementPort++
	}
}

func StartUnixServer(sockPath string, impl interface{}, opts ...grpc.ServerOption) {
	_ = fileutils.DeleteFile(sockPath)

	netAddr := dfnet.NetAddr{
		Type: dfnet.UNIX,
		Addr: sockPath,
	}

	if err := startServer(netAddr, impl, opts); err != nil {
		panic(err)
	}
}

// start server with addr and register source
func startServer(netAddr dfnet.NetAddr, impl interface{}, opts []grpc.ServerOption) error {
	lis, err := net.Listen(string(netAddr.Type), netAddr.Addr)

	if err != nil {
		return err
	}

	server := grpc.NewServer(append(serverOpts, opts...)...)

	switch netAddr.Type {
	case dfnet.UNIX:
		unixServer = server
	case dfnet.TCP:
		tcpServer = server
		addr := lis.Addr().String()
		index := strings.LastIndex(addr, ":")
		if p, err := strconv.Atoi(stringutils.SubString(addr, index+1, len(addr))); err != nil {
			return err
		} else {
			sp.port = p
			close(sp.ch)
		}
	}

	register(server, impl)

	return server.Serve(lis)
}

func StopServer() {
	if unixServer != nil {
		unixServer.Stop()
	}

	if tcpServer != nil {
		tcpServer.Stop()
	}
}

func isErrAddrInuse(err error) bool {
	if ope, ok := err.(*net.OpError); ok {
		if sse, ok := ope.Err.(*os.SyscallError); ok {
			if errno, ok := sse.Err.(syscall.Errno); ok {
				return errno == syscall.EADDRINUSE
			}
		}
	}

	return false
}

func ConvertServerError(err error) error {
	if err == nil {
		return nil
	}

	if status.Code(err) == codes.Unknown {
		return status.Error(codes.Aborted, err.Error())
	} else {
		return err
	}
}

type wrappedServerStream struct {
	grpc.ServerStream
	method string
}

func (w *wrappedServerStream) RecvMsg(m interface{}) error {
	err := w.ServerStream.RecvMsg(m)
	if err != nil {
		logger.GrpcLogger.Errorf("server receive a message:%T error:%v for method:%s", m, err, w.method)
	}

	return err
}

func (w *wrappedServerStream) SendMsg(m interface{}) error {
	err := w.ServerStream.SendMsg(m)
	if err != nil {
		logger.GrpcLogger.Errorf("server send a message:%T error:%v for method:%s", m, err, w.method)
	}

	return err
}

func streamServerInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	err := handler(srv, &wrappedServerStream{
		ServerStream: ss,
		method:       info.FullMethod,
	})
	if err != nil {
		logger.GrpcLogger.Errorf("create server stream error:%v for method:%s", err, info.FullMethod)
	}

	return err
}

func unaryServerInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	m, err := handler(ctx, req)
	if err != nil {
		logger.GrpcLogger.Errorf("do unary server error:%v for method:%s", err, info.FullMethod)
	}

	return m, err
}

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
func ListenWithPortRange(listen string, startPort, endPort int64) (net.Listener, int, error) {
	for ; startPort <= endPort; startPort++ {
		listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", listen, startPort))
		if err == nil && listener != nil {
			addr := listener.Addr().String()
			index := strings.LastIndex(addr, ":")
			p, cerr := strconv.Atoi(stringutils.SubString(addr, index+1, len(addr)))
			if cerr != nil {
				continue
			}
			return listener, p, nil
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
