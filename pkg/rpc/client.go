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
	"github.com/dragonflyoss/Dragonfly2/pkg/util/maths"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
	"reflect"
	"sync"
	"time"
)

type InitClientFunc func(*Connection)

type Connection struct {
	rwMutex   *sync.RWMutex
	curTarget string
	nextNum   int
	NetAddrs  []dfnet.NetAddr
	Conn      *grpc.ClientConn
	Ref       interface{}
	init      InitClientFunc
	opts      []grpc.DialOption
}

type RetryMeta struct {
	Times       int     // times of replacing stream on a client
	MaxAttempts int     // limit count for retry
	InitBackoff float64 // second
	MaxBackOff  float64 // second
}

var clientOpts = []grpc.DialOption{
	grpc.FailOnNonTempDialError(true),
	grpc.WithBlock(),
	grpc.WithInitialConnWindowSize(4 * 1024 * 1024),
	grpc.WithInsecure(),

	grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Time:                2 * time.Hour,
		Timeout:             10 * time.Second,
		PermitWithoutStream: true,
	}),
	grpc.WithStreamInterceptor(streamClientInterceptor),
	grpc.WithUnaryInterceptor(unaryClientInterceptor),
}

func BuildClient(client interface{}, init InitClientFunc, addrs []dfnet.NetAddr, opts []grpc.DialOption) (interface{}, error) {
	if len(addrs) == 0 || len(addrs) > 10 {
		return nil, errors.New("addrs are empty or greater than 10")
	}

	conn := &Connection{
		rwMutex:  new(sync.RWMutex),
		NetAddrs: addrs,
		Ref:      client,
		init:     init,
		opts:     opts,
	}

	if err := conn.connect(); err != nil {
		return nil, err
	}

	return client, nil
}

func (c *Connection) connect() error {
	if c.nextNum >= len(c.NetAddrs) {
		return errors.New("available addr is not found in the candidates")
	}

	if c.Ref == nil {
		return errors.New("client has already been closed")
	}

	var cc *grpc.ClientConn
	var err error

	for ; c.nextNum < len(c.NetAddrs); {
		ctx, _ := context.WithTimeout(context.Background(), 15*time.Second)
		cc, err = grpc.DialContext(ctx, c.NetAddrs[c.nextNum].GetEndpoint(), append(clientOpts, c.opts...)...)

		c.nextNum++

		if err == nil {
			c.Conn = cc
			c.curTarget = c.NetAddrs[c.nextNum-1].Addr
			c.init(c)
			break
		}
	}

	return err
}

// GetClientSafely returns client,target,nextNum
func (c *Connection) GetClientSafely() (interface{}, string, int) {
	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()

	return reflect.ValueOf(c.Ref).Elem().FieldByName("Client").Interface(), c.curTarget, c.nextNum
}

func (c *Connection) Close() error {
	c.rwMutex.Lock()
	defer c.rwMutex.Unlock()

	c.Ref = nil

	return c.Conn.Close()
}

func (c *Connection) TryMigrate(nextNum int, cause error) error {
	if status.Code(cause) == codes.Aborted {
		return cause
	}

	c.rwMutex.Lock()
	defer c.rwMutex.Unlock()

	if nextNum != c.nextNum {
		return nil
	}

	previousConn := c.Conn
	if err := c.connect(); err != nil {
		return err
	}

	if previousConn != nil {
		_ = previousConn.Close()
	}

	return nil
}

func ExecuteWithRetry(f func() (interface{}, error), initBackoff float64, maxBackoff float64, maxAttempts int) (interface{}, error) {
	var res interface{}
	var err error
outer:
	for i := 0; i < maxAttempts; i++ {
		if i > 0 {
			time.Sleep(maths.RandBackoff(initBackoff, 2.0, maxBackoff, i))
		}

		res, err = f()
		if err == nil {
			break
		} else {
			switch status.Code(err) {
			case codes.Aborted, codes.FailedPrecondition:
				break outer
			}
		}
	}

	return res, err
}

type wrappedClientStream struct {
	grpc.ClientStream
	method string
	cc     *grpc.ClientConn
}

func (w *wrappedClientStream) RecvMsg(m interface{}) error {
	err := w.ClientStream.RecvMsg(m)
	if err != nil {
		logger.GrpcLogger.Errorf("client receive a message:%T error:%v for method:%s target:%s conn:%s", m, err, w.method, w.cc.Target(), w.cc.GetState().String())
	}
	return err
}

func (w *wrappedClientStream) SendMsg(m interface{}) error {
	err := w.ClientStream.SendMsg(m)
	if err != nil {
		logger.GrpcLogger.Errorf("client send a message:%T error:%v for method:%s target:%s conn:%s", m, err, w.method, w.cc.Target(), w.cc.GetState().String())
	}
	return err
}

func streamClientInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	s, err := streamer(ctx, desc, cc, method, opts...)
	if err != nil {
		logger.GrpcLogger.Errorf("create client stream error:%v for method:%s target:%s conn:%s", err, method, cc.Target(), cc.GetState().String())
		return nil, err
	}
	return &wrappedClientStream{
		ClientStream: s,
		method:       method,
		cc:           cc,
	}, nil
}

func unaryClientInterceptor(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	err := invoker(ctx, method, req, reply, cc, opts...)
	if err != nil {
		logger.GrpcLogger.Errorf("do unary client error:%v for method:%s target:%s conn:%s", err, method, cc.Target(), cc.GetState().String())
	}
	return err
}
