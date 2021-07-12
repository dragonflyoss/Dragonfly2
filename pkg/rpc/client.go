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

	"d7y.io/dragonfly.v2/internal/dfcodes"
	"d7y.io/dragonfly.v2/internal/dferrors"
	logger "d7y.io/dragonfly.v2/internal/dflog"
	"d7y.io/dragonfly.v2/pkg/basic/dfnet"
	"d7y.io/dragonfly.v2/pkg/synclock"
	"github.com/pkg/errors"
	"github.com/serialx/hashring"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
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

// todo Perfect state
type ConnStatus string

type Connection struct {
	ctx            context.Context
	cancelFun      context.CancelFunc
	rwMutex        *synclock.LockerPool
	dialOpts       []grpc.DialOption
	key2NodeMap    sync.Map // key -> node(many to one)
	node2ClientMap sync.Map // node -> clientConn(one to one)
	accessNodeMap  sync.Map // clientConn access time
	connExpireTime time.Duration
	gcConnTimeout  time.Duration
	gcConnInterval time.Duration
	dialTimeout    time.Duration
	name           string
	hashRing       *hashring.HashRing // server hash ring
	serverNodes    []dfnet.NetAddr
	status         ConnStatus
}

func newDefaultConnection(ctx context.Context) *Connection {
	childCtx, cancel := context.WithCancel(ctx)
	return &Connection{
		ctx:            childCtx,
		cancelFun:      cancel,
		rwMutex:        synclock.NewLockerPool(),
		dialOpts:       defaultClientOpts,
		key2NodeMap:    sync.Map{},
		node2ClientMap: sync.Map{},
		accessNodeMap:  sync.Map{},
		connExpireTime: defaultConnExpireTime,
		gcConnTimeout:  defaultGcConnTimeout,
		gcConnInterval: defaultGcConnInterval,
		dialTimeout:    defaultDialTimeout,
	}
}

var defaultClientOpts = []grpc.DialOption{
	grpc.FailOnNonTempDialError(true),
	grpc.WithBlock(),
	grpc.WithDisableServiceConfig(),
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

func NewConnection(ctx context.Context, name string, adders []dfnet.NetAddr, connOpts []ConnOption) *Connection {
	conn := newDefaultConnection(ctx)
	conn.name = name
	addresses := make([]string, 0, len(adders))
	for _, addr := range adders {
		addresses = append(addresses, addr.GetEndpoint())
	}
	conn.hashRing = hashring.New(addresses)
	conn.serverNodes = adders
	for _, opt := range connOpts {
		opt.apply(conn)
	}
	go conn.startGC()
	return conn
}

func (conn *Connection) CorrectKey2NodeRelation(tmpHashKey, realHashKey string) {
	if tmpHashKey == realHashKey {
		return
	}
	key, _ := conn.key2NodeMap.Load(tmpHashKey)
	serverNode := key.(string)
	conn.rwMutex.Lock(serverNode, false)
	defer conn.rwMutex.UnLock(serverNode, false)
	conn.key2NodeMap.Store(realHashKey, serverNode)
	conn.key2NodeMap.Delete(tmpHashKey)
}

func (conn *Connection) UpdateAccessNodeMapByHashKey(key string) {
	node, ok := conn.key2NodeMap.Load(key)
	if ok {
		conn.accessNodeMap.Store(node, time.Now())
		logger.With("conn", conn.name).Debugf("successfully update server node %s access time for hashKey %s", node, key)
		_, ok := conn.node2ClientMap.Load(node)
		if !ok {
			logger.With("conn", conn.name).Warnf("successfully update server node %s access time for hashKey %s,"+
				"but cannot found client conn in node2ClientMap", node, key)
		}
	} else {
		logger.With("conn", conn.name).Errorf("update access node map failed, hash key (%s) not found in key2NodeMap", key)
	}
}

func (conn *Connection) UpdateAccessNodeMapByServerNode(serverNode string) {
	conn.accessNodeMap.Store(serverNode, time.Now())
}

func (conn *Connection) AddServerNodes(addrs []dfnet.NetAddr) error {
	for _, addr := range addrs {
		serverNode := addr.GetEndpoint()
		conn.rwMutex.Lock(serverNode, false)
		conn.hashRing = conn.hashRing.AddNode(serverNode)
		conn.rwMutex.UnLock(serverNode, false)
		logger.With("conn", conn.name).Debugf("success add %s to server node list", addr)
	}
	return nil
}

// findCandidateClientConn find candidate node client conn other than exclusiveNodes
func (conn *Connection) findCandidateClientConn(key string, exclusiveNodes ...string) (*candidateClient, error) {
	ringNodes, ok := conn.hashRing.GetNodes(key, conn.hashRing.Size())
	if !ok {
		return nil, dferrors.ErrNoCandidateNode
	}
	candidateNodes := make([]string, 0, 0)
	for _, ringNode := range ringNodes {
		candidate := true
		for _, exclusiveNode := range exclusiveNodes {
			if exclusiveNode == ringNode {
				candidate = false
			}
		}
		if candidate {
			candidateNodes = append(candidateNodes, ringNode)
		}
	}
	logger.With("conn", conn.name).Infof("candidate result for hash key %s: all server node list:%v, exclusiveNodes node list:%v, candidate node list:%v",
		key, ringNodes, exclusiveNodes, candidateNodes)
	for _, candidateNode := range candidateNodes {
		conn.rwMutex.Lock(candidateNode, true)
		// Check whether there is a corresponding mapping client in the node2ClientMap
		// todo 下面部分可以直接调用loadOrCreate方法，但是日志没有这么调用打印全
		if client, ok := conn.node2ClientMap.Load(candidateNode); ok {
			logger.With("conn", conn.name).Infof("hit cache candidateNode %s for hash key %s", candidateNode, key)
			conn.rwMutex.UnLock(candidateNode, true)
			return &candidateClient{
				node: candidateNode,
				Ref:  client,
			}, nil
		}
		logger.With("conn", conn.name).Debugf("attempt to connect candidateNode %s for hash key %s", candidateNode, key)
		clientConn, err := conn.createClient(candidateNode, append(defaultClientOpts, conn.dialOpts...)...)
		if err == nil {
			logger.With("conn", conn.name).Infof("success connect to candidateNode %s for hash key %s", candidateNode, key)
			conn.rwMutex.UnLock(candidateNode, true)
			return &candidateClient{
				node: candidateNode,
				Ref:  clientConn,
			}, nil
		}

		logger.With("conn", conn.name).Infof("failed to connect candidateNode %s for hash key %s: %v", candidateNode, key, err)
		conn.rwMutex.UnLock(candidateNode, true)
	}
	return nil, dferrors.ErrNoCandidateNode
}

type candidateClient struct {
	node string
	Ref  interface{}
}

func (conn *Connection) createClient(target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	// should not retry
	ctx, cancel := context.WithTimeout(context.Background(), conn.dialTimeout)
	defer cancel()
	return grpc.DialContext(ctx, target, opts...)
}

// GetServerNode
func (conn *Connection) GetServerNode(hashKey string) (string, bool) {
	node, ok := conn.key2NodeMap.Load(hashKey)
	serverNode := node.(string)
	if ok {
		return serverNode, true
	}
	return "unknown", false
}

func (conn *Connection) GetClientConnByTarget(node string) (*grpc.ClientConn, error) {
	logger.With("conn", conn.name).Debugf("start to get client conn by target %s", node)
	conn.rwMutex.Lock(node, true)
	defer conn.rwMutex.UnLock(node, true)
	clientConn, err := conn.loadOrCreateClientConnByNode(node)
	if err != nil {
		return nil, errors.Wrapf(err, "get client conn by conn %s", node)
	}
	logger.With("conn", conn.name).Debugf("successfully get %s client conn", node)
	return clientConn, nil
}

func (conn *Connection) loadOrCreateClientConnByNode(node string) (clientConn *grpc.ClientConn, err error) {
	defer func() {
		if desc := recover(); desc != nil {
			err = errors.Errorf("%v", desc)
		}
	}()
	conn.accessNodeMap.Store(node, time.Now())
	client, ok := conn.node2ClientMap.Load(node)
	if ok {
		logger.With("conn", conn.name).Debugf("hit cache clientConn associated with node %s", node)
		return client.(*grpc.ClientConn), nil
	}

	logger.With("conn", conn.name).Debugf("failed to load clientConn associated with node %s, attempt to create it", node)
	clientConn, err = conn.createClient(node, append(defaultClientOpts, conn.dialOpts...)...)
	if err == nil {
		logger.With("conn", conn.name).Infof("success connect to node %s", node)
		// bind
		conn.node2ClientMap.Store(node, clientConn)
		return clientConn, nil
	}

	return nil, errors.Wrapf(err, "cannot found clientConn associated with node %s and create client conn failed", node)
}

// GetClientConn get conn or bind hashKey to candidate node, don't do the migrate action
// stick whether hash key need already associated with specify node
func (conn *Connection) GetClientConn(hashKey string, stick bool) (*grpc.ClientConn, error) {
	logger.With("conn", conn.name).Debugf("start to get client conn hashKey %s, stick %t", hashKey, stick)
	node, ok := conn.key2NodeMap.Load(hashKey)
	if stick && !ok {
		// if request is stateful, hash key must exist in key2NodeMap
		return nil, fmt.Errorf("it is a stateful request， but cannot find hash key(%s) in key2NodeMap", hashKey)
	}
	if ok {
		// if exist
		serverNode := node.(string)
		conn.rwMutex.Lock(serverNode, true)
		clientConn, err := conn.loadOrCreateClientConnByNode(serverNode)
		conn.rwMutex.UnLock(serverNode, true)
		if err != nil {
			return nil, err
		}
		return clientConn, nil
	}
	logger.With("conn", conn.name).Infof("no server node associated with hash key %s was found, start find candidate", hashKey)
	// if absence
	client, err := conn.findCandidateClientConn(hashKey)
	if err != nil {
		return nil, errors.Wrapf(err, "find candidate client conn for hash key %s", hashKey)
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
	logger.With("conn", conn.name).Infof("start try migrate server node for key %s, cause err: %v", key, cause)
	// todo recover findCandidateClientConn error
	if e, ok := cause.(*dferrors.DfError); ok {
		if e.Code != dfcodes.ResourceLacked && e.Code != dfcodes.UnknownError {
			return "", cause
		}
	}
	currentNode := ""
	if currentNode, ok := conn.key2NodeMap.Load(key); ok {
		preNode = currentNode.(string)
		exclusiveNodes = append(exclusiveNodes, currentNode.(string))
	} else {
		logger.With("conn", conn.name).Warnf("failed to find server node for hash key %s", key)
	}
	client, err := conn.findCandidateClientConn(key, exclusiveNodes...)
	if err != nil {
		return "", errors.Wrapf(err, "find candidate client conn for hash key %s", key)
	}
	logger.With("conn", conn.name).Infof("successfully migrate hash key %s from server node %s to %s", key, currentNode, client.node)
	conn.rwMutex.Lock(client.node, false)
	defer conn.rwMutex.UnLock(client.node, false)
	conn.key2NodeMap.Store(key, client.node)
	conn.node2ClientMap.Store(client.node, client.Ref)
	conn.accessNodeMap.Store(client.node, time.Now())
	return
}

func (conn *Connection) Close() error {
	for i := range conn.serverNodes {
		serverNode := conn.serverNodes[i].GetEndpoint()
		conn.rwMutex.Lock(serverNode, false)
		conn.hashRing.RemoveNode(serverNode)
		value, ok := conn.node2ClientMap.Load(serverNode)
		if ok {
			clientCon := value.(*grpc.ClientConn)
			err := clientCon.Close()
			if err == nil {
				conn.node2ClientMap.Delete(serverNode)
			} else {
				logger.GrpcLogger.With("conn", conn.name).Warnf("failed to close clientConn:%s: %v", serverNode, err)
			}
		}
		// gc hash keys
		conn.key2NodeMap.Range(func(key, value interface{}) bool {
			if value == serverNode {
				conn.key2NodeMap.Delete(key)
				logger.GrpcLogger.With("conn", conn.name).Infof("success gc key:%s associated with server node %s", key, serverNode)
			}
			return true
		})
		conn.accessNodeMap.Delete(serverNode)
		conn.rwMutex.UnLock(serverNode, false)
	}
	conn.cancelFun()
	return nil
}

func (conn *Connection) UpdateState(adders []dfnet.NetAddr) {
	// todo lock
	conn.serverNodes = adders
	var addresses []string
	for _, addr := range adders {
		addresses = append(addresses, addr.GetEndpoint())
	}

	conn.hashRing = hashring.New(addresses)
}
