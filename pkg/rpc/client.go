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

	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/dferrors"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/synclock"
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

type Connection struct {
	ctx            context.Context
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
}

func newDefaultConnection(ctx context.Context) *Connection {
	return &Connection{
		ctx:            ctx,
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

func NewConnection(ctx context.Context, name string, addrs []dfnet.NetAddr, connOpts []ConnOption) *Connection {
	conn := newDefaultConnection(ctx)
	conn.name = name
	addresses := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		addresses = append(addresses, addr.GetEndpoint())
	}
	conn.hashRing = hashring.New(addresses)
	for _, opt := range connOpts {
		opt.apply(conn)
	}
	go conn.startGC()
	return conn
}

func (conn *Connection) CorrectKey2NodeRelation(serverNode, tmpHashKey, realHashKey string) {
	if tmpHashKey == realHashKey {
		return
	}
	conn.rwMutex.Lock(serverNode, false)
	defer conn.rwMutex.UnLock(serverNode, false)
	conn.key2NodeMap.Store(realHashKey, serverNode)
	conn.key2NodeMap.Delete(tmpHashKey)
}

func (conn *Connection) UpdateAccessNodeMap(key string) {
	node, ok := conn.key2NodeMap.Load(key)
	if ok {
		conn.accessNodeMap.Store(node, time.Now())
		_, ok := conn.node2ClientMap.Load(node)
		if !ok {
			logger.Warnf("conn[%s]: server node(%s) not found in node2ClientMap when update access node map", conn.name, node)
		}
	} else {
		logger.Errorf("conn[%s]: update access node map failed, hash key (%s) not found in key2NodeMap", conn.name, key)
	}
}

func (conn *Connection) AddServerNodes(addrs []dfnet.NetAddr) error {
	for _, addr := range addrs {
		serverNode := addr.GetEndpoint()
		conn.rwMutex.Lock(serverNode, false)
		conn.hashRing = conn.hashRing.AddNode(serverNode)
		conn.rwMutex.UnLock(serverNode, false)
		logger.Debugf("conn[%s]: success add %s to server list", conn.name, addr)
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
	logger.Infof("conn[%s]: all server node list:%v, exclusiveNodes node list:%v, candidate node list:%v", conn.name, ringNodes, exclusiveNodes,
		candidateNodes)
	for _, candidateNode := range candidateNodes {
		conn.rwMutex.Lock(candidateNode, true)
		// Check whether there is a corresponding mapping client in the node2ClientMap
		if client, ok := conn.node2ClientMap.Load(candidateNode); ok {
			logger.Infof("conn[%s]: hit cache candidateNode: %s", conn.name, candidateNode)
			conn.rwMutex.UnLock(candidateNode, true)
			return &candidateClient{
				node: candidateNode,
				Ref:  client,
			}, nil
		}
		logger.Debugf("conn[%s]: attempt to connect candidateNode: %s", conn.name, candidateNode)
		if clientConn, err := conn.createClient(candidateNode, append(defaultClientOpts, conn.dialOpts...)...); err == nil {
			logger.Infof("conn[%s]: success connect to candidateNode: %s", conn.name, candidateNode)
			conn.rwMutex.UnLock(candidateNode, true)
			return &candidateClient{
				node: candidateNode,
				Ref:  clientConn,
			}, nil
		} else {
			logger.Infof("conn[%s]: failed to connect candidateNode: %s: %v", conn.name, candidateNode, err)
		}
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

func (conn *Connection) GetServerNodes() []string {
	servers, _ := conn.hashRing.GetNodes("", conn.hashRing.Size())
	return servers
}

func (conn *Connection) GetClientConnByTarget(node string) (*grpc.ClientConn, error) {
	conn.accessNodeMap.Store(node, time.Now())
	conn.rwMutex.Lock(node, true)
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
	clientConn, err := conn.createClient(node, append(defaultClientOpts, conn.dialOpts...)...)
	if err != nil {
		return nil, errors.Wrapf(err, "conn[%s]: failed to find candidate client conn", conn.name)
	}
	conn.node2ClientMap.Store(node, clientConn)
	return clientConn, nil
}

func (conn *Connection) loadOrCreateClientConnByNode(node string) (clientConn *grpc.ClientConn, err error) {
	defer func() {
		if desc := recover(); desc != nil {
			err = errors.Errorf("%v", desc)
		}
	}()
	client, ok := conn.node2ClientMap.Load(node)
	if ok {
		return client.(*grpc.ClientConn), nil
	}
	if clientConn, err := conn.createClient(node, append(defaultClientOpts, conn.dialOpts...)...); err == nil {
		// bind
		conn.node2ClientMap.Store(node, clientConn)
		return clientConn, nil
	} else {
		return nil, errors.Wrapf(err, "cannot found clientConn associated with node %s and create client conn failed", node)
	}
}

// GetClientConn get conn or bind hashKey to candidate node, don't do the migrate action
// hasState whether has state
func (conn *Connection) GetClientConn(hashKey string, hasState bool) (*grpc.ClientConn, error) {
	node, ok := conn.key2NodeMap.Load(hashKey)
	if hasState && !ok {
		// if request is stateful, hash key must exist in key2NodeMap
		return nil, fmt.Errorf("conn[%s]: it is a stateful request， but cannot find hash key(%s) in key2NodeMap", hashKey)
	}
	if ok {
		// if exist
		serverNode := node.(string)
		conn.rwMutex.Lock(serverNode, true)
		conn.accessNodeMap.Store(node, time.Now())
		clientConn, err := conn.loadOrCreateClientConnByNode(serverNode)
		conn.rwMutex.UnLock(serverNode, true)
		if err != nil {
			return nil, err
		}
		return clientConn, nil
	}

	// if absence
	client, err := conn.findCandidateClientConn(hashKey)
	if err != nil {
		return nil, errors.Wrapf(err, "conn[%s]: failed to find candidate client conn", conn.name)
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
	logger.Infof("conn[%s]: start try migrate server node for key %s, cause err: %v", conn.name, key, cause)
	// todo recover findCandidateClientConn error
	if e, ok := cause.(*dferrors.DfError); ok {
		if e.Code != dfcodes.ResourceLacked && e.Code != dfcodes.UnknownError {
			return "", cause
		}
	}
	// only resourceLack or unknown err will migrate
	currentNode := ""
	if currentNode, ok := conn.key2NodeMap.Load(key); ok {
		preNode = currentNode.(string)
		exclusiveNodes = append(exclusiveNodes, currentNode.(string))
	} else {
		logger.Warnf("conn[%s]: failed to find server node for key %s", conn.name, key)
	}
	client, err := conn.findCandidateClientConn(key, exclusiveNodes...)
	if err != nil {
		return "", errors.Wrapf(err, "conn[%s]: failed to find candidate client conn", conn.name)
	}
	logger.Infof("conn[%s]: successfully migrate hash key %s from server node %s to %s", conn.name, key, currentNode, client.node)
	conn.rwMutex.Lock(client.node, false)
	defer conn.rwMutex.UnLock(client.node, false)
	conn.key2NodeMap.Store(key, client.node)
	conn.node2ClientMap.Store(client.node, client.Ref)
	conn.accessNodeMap.Store(client.node, time.Now())
	return
}
