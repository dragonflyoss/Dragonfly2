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
	"io"
	"sync"
	"time"

	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/dferrors"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/structure/syncmap"
	"d7y.io/dragonfly/v2/pkg/synclock"
	"d7y.io/dragonfly/v2/pkg/util/mathutils"
	"github.com/pkg/errors"
	"github.com/serialx/hashring"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

const (
	// gcConnectionsTimeout specifies the timeout for clientConn gc.
	// If the actual execution time exceeds this threshold, a warning will be thrown.
	gcConnectionsTimeout = 1.0 * time.Second

	// gcConnectionsInterval
	gcConnectionsInterval = 60 * time.Second

	// connExpireTime
	connExpireTime = 5 * time.Minute
)

type Connection struct {
	name           string
	lock           sync.Mutex
	rwMutex        *synclock.KeyLocker
	opts           []grpc.DialOption
	key2NodeMap    sync.Map           // key -> node(many to one)
	node2ClientMap sync.Map           // node -> clientConn(one to one)
	hashRing       *hashring.HashRing // server hash ring
	accessNodeMap  *syncmap.SyncMap   // clientConn access time
	connExpireTime time.Duration
}

type RetryMeta struct {
	StreamTimes int     // times of replacing stream on the current client
	MaxAttempts int     // limit times for execute
	InitBackoff float64 // second
	MaxBackOff  float64 // second
}

func NewConnection(name string, addrs []dfnet.NetAddr, opts ...grpc.DialOption) *Connection {
	opts = append(clientOpts, opts...)
	addresses := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		addresses = append(addresses, addr.GetEndpoint())
	}
	conn := &Connection{
		name:           name,
		rwMutex:        synclock.NewKeyLocker(),
		opts:           opts,
		hashRing:       hashring.New(addresses),
		accessNodeMap:  syncmap.NewSyncMap(),
		connExpireTime: connExpireTime,
	}
	go conn.startGC(context.TODO())
	return conn
}

func (conn *Connection) UpdateAccessNodeMap(key string) {
	node, ok := conn.key2NodeMap.Load(key)
	if ok {
		_, ok := conn.node2ClientMap.Load(node)
		if ok {
			conn.accessNodeMap.Store(node, time.Now())
			return
		} else {
			logger.GrpcLogger.Warnf("conn:%s failed to get node(%s) from node2ClientMap", conn.name, node)
		}
	} else {
		logger.GrpcLogger.Warnf("conn:%s failed to get key(%s) from key2NodeMap", conn.name, key)
	}
}

func (conn *Connection) Recv(stream interface{}) (item interface{}, err error) {
	//streamValue := reflect.ValueOf(stream)
	//hashKey := streamValue.FieldByName("HashKey")
	//item = streamValue.MethodByName("Recv").Call([]reflect.Value{})
	//conn.UpdateAccessNodeMap(hashKey.Interface().(string))
	return nil, nil
}

func (conn *Connection) Send() {

}

func (conn *Connection) AddNodes(addrs []dfnet.NetAddr) error {
	conn.lock.Lock()
	defer conn.lock.Unlock()
	for _, addr := range addrs {
		conn.hashRing = conn.hashRing.AddNode(addr.GetEndpoint())
		logger.Debugf("conn:%s success add %s to server node list", conn.name, addr)
	}
	return nil
}

var clientOpts = []grpc.DialOption{
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

func (conn *Connection) startGC(ctx context.Context) {
	// todo 从hashing环中删除频繁失败的节点
	logger.GrpcLogger.Debugf("conn:%s start the gc connections job", conn.name)
	// execute the GC by fixed delay
	ticker := time.NewTicker(gcConnectionsInterval)
	for range ticker.C {
		removedConnCount := 0
		startTime := time.Now()
		// range all connections and determine whether they are expired
		// todo use anther locker, @santong
		//conn.rwMutex.Lock()
		nodes := conn.accessNodeMap.ListKeyAsStringSlice()
		totalNodeSize := len(nodes)
		for _, node := range nodes {
			atime, err := conn.accessNodeMap.GetAsTime(node)
			if err != nil {
				logger.GrpcLogger.Errorf("conn:%s gc connections: failed to get access time node(%s): %v", conn.name,
					node, err)
				continue
			}
			if time.Since(atime) < conn.connExpireTime {
				continue
			}
			conn.gcConn(ctx, node)
			removedConnCount++
		}
		// todo use anther locker, @santong
		//conn.rwMutex.Unlock()
		// slow GC detected, report it with a log warning
		if timeDuring := time.Since(startTime); timeDuring > gcConnectionsTimeout {
			logger.GrpcLogger.Warnf("conn:%s gc connections:%d cost:%.3f", conn.name, removedConnCount,
				timeDuring.Seconds())
		}
		logger.GrpcLogger.Infof("conn:%s gc connections: success to gc clientConn count(%d), remainder count(%d)",
			conn.name, removedConnCount, totalNodeSize-removedConnCount)
	}
}

// GetClient
func (conn *Connection) GetServerNode(hashKey string) string {
	node, ok := conn.key2NodeMap.Load(hashKey)
	if ok {
		return node.(string)
	}
	return "unknown"
}

func (conn *Connection) GetClientConnByTarget(node string) (*grpc.ClientConn, error) {
	conn.rwMutex.Lock(node, true)
	conn.accessNodeMap.Store(node, time.Now())
	if client, ok := conn.node2ClientMap.Load(node); ok {
		conn.rwMutex.UnLock(node, true)
		return client.(*grpc.ClientConn), nil
	}
	conn.rwMutex.UnLock(node, true)

	// reconfirm
	conn.rwMutex.Lock(node, false)
	defer conn.rwMutex.UnLock(node, false)
	if client, ok := conn.node2ClientMap.Load(node); ok {
		return client.(*grpc.ClientConn), nil
	}
	conn.hashRing = conn.hashRing.AddNode(node)
	clientConn, err := conn.createClient(node, append(clientOpts, conn.opts...)...)
	if err != nil {
		return nil, errors.Wrapf(err, "conn:%s failed to find candidate client conn", conn.name)
	}
	conn.node2ClientMap.Store(node, clientConn)
	return clientConn, nil
}

// GetClient
func (conn *Connection) GetClientConn(hashKey string) (*grpc.ClientConn, error) {
	conn.rwMutex.Lock(hashKey, true)
	node, ok := conn.key2NodeMap.Load(hashKey)
	if ok {
		conn.accessNodeMap.Store(node, time.Now())
		client, ok := conn.node2ClientMap.Load(node)
		if ok {
			conn.rwMutex.UnLock(hashKey, true)
			return client.(*grpc.ClientConn), nil
		}
	}
	conn.rwMutex.UnLock(hashKey, true)
	// reconfirm
	conn.rwMutex.Lock(hashKey, false)
	defer conn.rwMutex.UnLock(hashKey, false)
	node, ok = conn.key2NodeMap.Load(hashKey)
	if ok {
		conn.accessNodeMap.Store(node, time.Now())
		client, ok := conn.node2ClientMap.Load(node)
		if ok {
			return client.(*grpc.ClientConn), nil
		}
	}
	client, err := conn.findCandidateClientConn(hashKey)
	if err != nil {
		return nil, errors.Wrapf(err, "conn:%s failed to find candidate client conn", conn.name)
	}
	conn.rwMutex.Lock(client.node, false)
	defer conn.rwMutex.UnLock(client.node, false)
	conn.key2NodeMap.Store(hashKey, client.node)
	conn.node2ClientMap.Store(client.node, client.Ref)
	conn.accessNodeMap.Store(client.node, time.Now())
	return client.Ref.(*grpc.ClientConn), nil
}

// TryMigrate migrate key to another hash node other than exclusiveNodes
// preNode node before the migration
func (conn *Connection) TryMigrate(key string, cause error, exclusiveNodes []string) (preNode string, err error) {
	// todo recover findCandidateClientConn error
	if e, ok := cause.(*dferrors.DfError); ok {
		if e.Code != dfcodes.ResourceLacked && e.Code != dfcodes.UnknownError {
			return "", cause
		}
	}
	if currentNode, ok := conn.key2NodeMap.Load(key); ok {
		preNode = currentNode.(string)
		exclusiveNodes = append(exclusiveNodes, currentNode.(string))
	} else {
		logger.GrpcLogger.Warnf("conn:%s failed to find server node for key %s", conn.name, key)
	}
	client, err := conn.findCandidateClientConn(key, exclusiveNodes...)
	if err != nil {
		return "", errors.Wrapf(err, "conn:%s failed to find candidate client conn", conn.name)
	}
	conn.rwMutex.Lock(client.node, false)
	defer conn.rwMutex.UnLock(client.node, false)
	conn.key2NodeMap.Store(key, client.node)
	conn.node2ClientMap.Store(client.node, client.Ref)
	conn.accessNodeMap.Store(client.node, time.Now())
	return
}

func ExecuteWithRetry(f func() (interface{}, error), initBackoff float64, maxBackoff float64, maxAttempts int, cause error) (interface{}, error) {
	var res interface{}
	for i := 0; i < maxAttempts; i++ {
		if e, ok := cause.(*dferrors.DfError); ok {
			if e.Code != dfcodes.UnknownError {
				return res, cause
			}
		}

		if i > 0 {
			time.Sleep(mathutils.RandBackoff(initBackoff, maxBackoff, 2.0, i))
		}

		res, cause = f()
		if cause == nil {
			break
		}
	}

	return res, cause
}

type wrappedClientStream struct {
	grpc.ClientStream
	method string
	cc     *grpc.ClientConn
}

func (w *wrappedClientStream) RecvMsg(m interface{}) error {
	err := w.ClientStream.RecvMsg(m)
	if err != nil && err != io.EOF {
		err = convertClientError(err)
		logger.GrpcLogger.Errorf("client receive a message:%T error:%v for method:%s target:%s connState:%s", m, err, w.method, w.cc.Target(), w.cc.GetState().String())
	}

	return err
}

func (w *wrappedClientStream) SendMsg(m interface{}) error {
	err := w.ClientStream.SendMsg(m)
	if err != nil {
		logger.GrpcLogger.Errorf("client send a message:%T error:%v for method:%s target:%s connState:%s", m, err, w.method, w.cc.Target(), w.cc.GetState().String())
	}

	return err
}

func streamClientInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	s, err := streamer(ctx, desc, cc, method, opts...)
	if err != nil {
		logger.GrpcLogger.Errorf("create client stream error:%v for method:%s target:%s connState:%s", err, method, cc.Target(), cc.GetState().String())
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
		err = convertClientError(err)
		logger.GrpcLogger.Errorf("do unary client error:%v for method:%s target:%s connState:%s", err, method, cc.Target(), cc.GetState().String())
	}

	return err
}

func convertClientError(err error) error {
	s := status.Convert(err)
	if s != nil {
		for _, d := range s.Details() {
			switch internal := d.(type) {
			case *base.ResponseState:
				return &dferrors.DfError{
					Code:    internal.Code,
					Message: internal.Msg,
				}
			}
		}
	}

	return err
}
