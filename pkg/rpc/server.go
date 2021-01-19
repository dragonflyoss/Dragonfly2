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
	"github.com/dragonflyoss/Dragonfly2/pkg/basic/dfnet"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/fileutils"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/stringutils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type RegisterFunc func(*grpc.Server, interface{})

var (
	rf         RegisterFunc
	tcpServer  *grpc.Server
	unixServer *grpc.Server
	mutex      = sync.Mutex{}
)

func SetRegister(f RegisterFunc) {
	if f == nil {
		return
	}

	mutex.Lock()
	defer mutex.Unlock()

	if rf != nil {
		panic("duplicated service register")
	}

	rf = f
}

var serverOpts = []grpc.ServerOption{
	grpc.ConnectionTimeout(10 * time.Second),
	grpc.InitialConnWindowSize(4 * 1024 * 1024),
	grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
		MinTime: 10 * time.Minute,
	}),
	grpc.KeepaliveParams(keepalive.ServerParameters{
		MaxConnectionIdle: 3 * time.Minute,
	}),
	grpc.MaxConcurrentStreams(100),
	grpc.StreamInterceptor(streamServerInterceptor),
	grpc.UnaryInterceptor(unaryServerInterceptor),
}

var sp = struct {
	port int
	ch   chan struct{}
	once sync.Once
}{ch: make(chan struct{}, 1)}

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
			Addr: ":" + strconv.Itoa(incrementPort),
		}

		if err := startServer(netAddr, impl, opts...); err != nil && !isErrAddrInuse(err) {
			panic(err)
		} else if err == nil {
			return
		}

		incrementPort++
	}
}

func StartUnixServer(path string, impl interface{}, opts ...grpc.ServerOption) {
	_ = fileutils.DeleteFile(path)

	netAddr := dfnet.NetAddr{
		Type: dfnet.UNIX,
		Addr: path,
	}

	if err := startServer(netAddr, impl, opts...); err != nil {
		panic(err)
	}
}

// start server with addr and register source
func startServer(netAddr dfnet.NetAddr, impl interface{}, opts ...grpc.ServerOption) error {
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
			sp.ch <- struct{}{}
		}
	}

	rf(server, impl)

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
