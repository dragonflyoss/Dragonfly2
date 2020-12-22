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
	"github.com/dragonflyoss/Dragonfly2/pkg/basic"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
	"net"
	"sync"
	"time"
)

type RegisterFunc func(*grpc.Server, interface{})

var (
	rf         RegisterFunc
	grpcServer *grpc.Server
	mutex      = sync.Mutex{}
)

func SetRegister(f RegisterFunc) {
	if f == nil {
		return
	}

	mutex.Lock()
	defer mutex.Unlock()

	if rf != nil {
		panic("duplicated registration")
	}

	rf = f
}

var serverOpts = []grpc.ServerOption{
	grpc.ConnectionTimeout(10 * time.Second),
	grpc.InitialConnWindowSize(4 * 1024 * 1024),
	grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
		MinTime:             10 * time.Minute,
		PermitWithoutStream: true,
	}),
	grpc.KeepaliveParams(keepalive.ServerParameters{
		MaxConnectionIdle: 10 * time.Minute,
	}),
	grpc.MaxConcurrentStreams(100),
	grpc.NumStreamWorkers(20000),
	grpc.StreamInterceptor(streamServerInterceptor),
	grpc.UnaryInterceptor(unaryServerInterceptor),
}

// start server with addr and register source
func StartServer(netAddr basic.NetAddr, impl interface{}) error {
	lis, err := net.Listen(string(netAddr.Type), netAddr.Addr)
	if err != nil {
		return err
	}

	grpcServer = grpc.NewServer(serverOpts...)

	rf(grpcServer, impl)

	return grpcServer.Serve(lis)
}

func StopServer() {
	grpcServer.Stop()
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
