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
	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"fmt"
	"google.golang.org/grpc"
	"time"
)

type candidateClient struct {
	node string
	Ref  interface{}
}

// findCandidateClientConn find candidate node client conn other than exclusiveNodes
func (conn *Connection) findCandidateClientConn(key string, exclusiveNodes ...string) *candidateClient {
	ringNodes, ok := conn.HashRing.GetNodes(key, conn.HashRing.Size())
	if !ok {
		panic(fmt.Sprintf("failed to get hash node list for key: %s", key))
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
	for _, candidateNode := range candidateNodes {
		// Check whether there is a corresponding mapping client in the node2ClientMap
		if client, ok := conn.node2ClientMap.Load(candidateNode); ok {
			return &candidateClient{
				node: candidateNode,
				Ref:  client,
			}
		}
		if clientConn, err := conn.createClient(&dfnet.NetAddr{
			Type: conn.networkType,
			Addr: candidateNode,
		}, append(clientOpts, conn.opts...)...); err == nil {
			return &candidateClient{
				node: candidateNode,
				Ref:  clientConn,
			}
		} else {
			logger.GrpcLogger.Warnf("failed to create client for %s: %v", candidateNode, err)
		}
	}
	panic(fmt.Sprintf("failed to create candidate client for key: %s", key))
}

func (conn *Connection) createClient(netAddr *dfnet.NetAddr, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	// should not retry
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return grpc.DialContext(ctx, netAddr.GetEndpoint(), opts...)
}

func (conn *Connection) gcConn(ctx context.Context, node string) {
	value, ok := conn.node2ClientMap.Load(node)
	if ok {
		clientCon := value.(*grpc.ClientConn)
		clientCon.Close()
		conn.node2ClientMap.Delete(node)
	} else {
		logger.GrpcLogger.Warnf("")
	}
	conn.key2NodeMap.Range(func(key, value interface{}) bool {
		if value == node {
			conn.key2NodeMap.Delete(key)
		}
		return true
	})
}
