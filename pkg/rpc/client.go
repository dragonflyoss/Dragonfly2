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
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
)

const (
	defaultDialTimeout = 1 * time.Minute
)

type Closer interface {
	Close() error
}

type Connection struct {
	ctx            context.Context
	cancel         context.CancelFunc
	rwMutex        sync.RWMutex
	dialOpts       []grpc.DialOption
	connExpireTime time.Duration
	dialTimeout    time.Duration
	scheme         string
	serverNodes    []dfnet.NetAddr
}

func newDefaultConnection(ctx context.Context) *Connection {
	childCtx, cancel := context.WithCancel(ctx)
	return &Connection{
		ctx:         childCtx,
		cancel:      cancel,
		dialOpts:    defaultClientOpts,
		dialTimeout: defaultDialTimeout,
	}
}

var defaultClientOpts = []grpc.DialOption{
	grpc.FailOnNonTempDialError(true),
	grpc.WithBlock(),
	grpc.WithInitialConnWindowSize(8 * 1024 * 1024),
	grpc.WithInsecure(),
	grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Time:    2 * time.Minute,
		Timeout: 10 * time.Second,
	}),
	grpc.WithStreamInterceptor(streamClientInterceptor),
	grpc.WithUnaryInterceptor(unaryClientInterceptor),
}

type ConnOption interface {
	apply(*Connection)
}

type funcConnOption struct {
	f func(*Connection)
}

func (fdo *funcConnOption) apply(conn *Connection) {
	fdo.f(conn)
}

func newFuncConnOption(f func(option *Connection)) *funcConnOption {
	return &funcConnOption{
		f: f,
	}
}

func WithDialOption(opts []grpc.DialOption) ConnOption {
	return newFuncConnOption(func(conn *Connection) {
		conn.dialOpts = append(defaultClientOpts, opts...)
	})
}

func WithDialTimeout(dialTimeout time.Duration) ConnOption {
	return newFuncConnOption(func(conn *Connection) {
		conn.dialTimeout = dialTimeout
	})
}

func NewConnection(ctx context.Context, scheme string, addrs []dfnet.NetAddr, connOpts []ConnOption) *Connection {
	conn := newDefaultConnection(ctx)
	conn.scheme = scheme
	conn.serverNodes = addrs
	for _, opt := range connOpts {
		opt.apply(conn)
	}
	if resolver, ok := Scheme2Resolver[scheme]; ok {
		resolver.UpdateAddrs(addrs)
	}
	// If there is not a resolver for the scheme, it is the grpc's default situation, so it does not need to return error.
	return conn
}

func (conn *Connection) AddServerNodes(addrs []dfnet.NetAddr) error {
	conn.rwMutex.Lock()
	defer conn.rwMutex.Unlock()
	conn.serverNodes = append(conn.serverNodes, addrs...)
	if resolver, ok := Scheme2Resolver[conn.scheme]; ok {
		resolver.UpdateAddrs(addrs)
	}
	return nil
}

func (conn *Connection) NewConsistentHashClient(target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	// should not retry
	ctx, cancel := context.WithTimeout(conn.ctx, conn.dialTimeout)
	defer cancel()

	opts = append(append(conn.dialOpts, grpc.WithDefaultServiceConfig(fmt.Sprintf(`{
		"loadBalancingPolicy": "%s"
	}`, d7yBalancerPolicy))), opts...)
	return grpc.DialContext(ctx, target, opts...)
}

func (conn *Connection) NewDirectClient(target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	// should not retry
	ctx, cancel := context.WithTimeout(conn.ctx, conn.dialTimeout)
	defer cancel()

	opts = append(append(conn.dialOpts, grpc.WithDisableServiceConfig()), opts...)
	return grpc.DialContext(ctx, target, opts...)
}

func (conn *Connection) Close() error {
	conn.cancel()
	return nil
}

func (conn *Connection) UpdateState(addrs []dfnet.NetAddr) {
	conn.rwMutex.Lock()
	defer conn.rwMutex.Unlock()
	updateFlag := false
	if len(addrs) != len(conn.serverNodes) {
		updateFlag = true
	} else {
		for i := 0; i < len(addrs); i++ {
			if addrs[i] != conn.serverNodes[i] {
				updateFlag = true
				break
			}
		}
	}
	if updateFlag {
		conn.serverNodes = addrs
		if resolver, ok := Scheme2Resolver[conn.scheme]; ok {
			resolver.UpdateAddrs(addrs)
		}
	}
}
