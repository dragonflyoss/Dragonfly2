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
	// defaultGcConnTimeout specifies the timeout for clientConn gc.
	// If the actual execution time exceeds this threshold, a warning will be thrown.
	defaultGcConnTimeout = 1.0 * time.Second

	defaultGcConnInterval = 60 * time.Second

	defaultConnExpireTime = 2 * time.Minute

	defaultDialTimeout = 10 * time.Second
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
	gcConnTimeout  time.Duration
	gcConnInterval time.Duration
	dialTimeout    time.Duration
	scheme         string
	serverNodes    []dfnet.NetAddr
}

func newDefaultConnection(ctx context.Context) *Connection {
	childCtx, cancel := context.WithCancel(ctx)
	return &Connection{
		ctx:            childCtx,
		cancel:         cancel,
		dialOpts:       defaultClientOpts,
		connExpireTime: defaultConnExpireTime,
		gcConnTimeout:  defaultGcConnTimeout,
		gcConnInterval: defaultGcConnInterval,
		dialTimeout:    defaultDialTimeout,
	}
}

var defaultClientOpts = []grpc.DialOption{
	grpc.WithDefaultServiceConfig(fmt.Sprintf(`{
		"loadBalancingPolicy": "%s"
	}`, d7yBalancerPolicy)),
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

func WithConnExpireTime(duration time.Duration) ConnOption {
	return newFuncConnOption(func(conn *Connection) {
		conn.connExpireTime = duration
	})
}

func WithDialOption(opts []grpc.DialOption) ConnOption {
	return newFuncConnOption(func(conn *Connection) {
		conn.dialOpts = append(defaultClientOpts, opts...)
	})
}

func WithGcConnTimeout(gcConnTimeout time.Duration) ConnOption {
	return newFuncConnOption(func(conn *Connection) {
		conn.gcConnTimeout = gcConnTimeout
	})
}

func WithGcConnInterval(gcConnInterval time.Duration) ConnOption {
	return newFuncConnOption(func(conn *Connection) {
		conn.gcConnInterval = gcConnInterval
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

func (conn *Connection) NewClient(target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	// should not retry
	ctx, cancel := context.WithTimeout(context.Background(), conn.dialTimeout)
	defer cancel()
	return grpc.DialContext(ctx, target, opts...)
}

// TryMigrate migrate key to another hash node other than exclusiveNodes
// preNode node before the migration
func (conn *Connection) TryMigrate(key string, cause error, exclusiveNodes []string) (preNode string, err error) {
	// TODO(zzy987)
	return
}

func (conn *Connection) Close() error {
	conn.cancel()
	return nil
}

func (conn *Connection) UpdateState(addrs []dfnet.NetAddr) {
	conn.rwMutex.Lock()
	defer conn.rwMutex.Unlock()
	conn.serverNodes = addrs
	if resolver, ok := Scheme2Resolver[conn.scheme]; ok {
		resolver.UpdateAddrs(addrs)
	}
}
