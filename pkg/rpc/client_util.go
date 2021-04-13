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
	"time"

	"d7y.io/dragonfly/v2/pkg/dferrors"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"google.golang.org/grpc"
)

type candidateClient struct {
	node string
	Ref  interface{}
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
	logger.GrpcLogger.Debugf("conn:%s all server node list:%v, exclusiveNodes node list:%v, "+
		"candidate node list:%v", conn.name, ringNodes, exclusiveNodes, candidateNodes)
	for _, candidateNode := range candidateNodes {
		// Check whether there is a corresponding mapping client in the node2ClientMap
		if client, ok := conn.node2ClientMap.Load(candidateNode); ok {
			logger.GrpcLogger.Debugf("conn:%s hit cache candidateNode: %s", conn.name, candidateNode)
			return &candidateClient{
				node: candidateNode,
				Ref:  client,
			}, nil
		}
		logger.GrpcLogger.Debugf("conn:%s attempt to connect candidateNode: %s", conn.name, candidateNode)
		if clientConn, err := conn.createClient(candidateNode, append(clientOpts, conn.opts...)...); err == nil {
			logger.GrpcLogger.Debugf("conn:%s success connect to candidateNode: %s", conn.name, candidateNode)
			return &candidateClient{
				node: candidateNode,
				Ref:  clientConn,
			}, nil
		} else {
			logger.GrpcLogger.Warnf("conn:%s failed to connect candidateNode: %s: %v", conn.name, candidateNode, err)
		}
	}
	return nil, dferrors.ErrNoCandidateNode
}

func (conn *Connection) createClient(target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	// should not retry
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return grpc.DialContext(ctx, target, opts...)
}

func (conn *Connection) gcConn(ctx context.Context, node string) {
	value, ok := conn.node2ClientMap.Load(node)
	if ok {
		clientCon := value.(*grpc.ClientConn)
		clientCon.Close()
		conn.node2ClientMap.Delete(node)
	} else {
		logger.GrpcLogger.Warnf("conn:%s node:%s dose not found", conn.name, node)
	}
	conn.key2NodeMap.Range(func(key, value interface{}) bool {
		if value == node {
			conn.key2NodeMap.Delete(key)
			logger.GrpcLogger.Infof("conn:%s success gc node:%s", conn.name, key)
		}
		return true
	})
}
