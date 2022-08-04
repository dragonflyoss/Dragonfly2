/*
 *     Copyright 2022 The Dragonfly Authors
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

package consistent

import (
	"sync"

	"github.com/serialx/hashring"
	"go.uber.org/zap"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/connectivity"

	logger "d7y.io/dragonfly/v2/internal/dflog"
)

type CtxKeyType string

func BuildPicker(conns ConnPool) balancer.Picker {
	log := logger.GrpcLogger.With("component", "balancer")
	log.Infof("balance picker Build called with info: %v", conns)
	if conns.Len() == 0 {
		return base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}

	hashRing := hashring.New(conns.ValidKeys())

	return &consistentHashRingPicker{
		SugaredLogger: logger.GrpcLogger.With("component", "consistent-hash"),
		hashRing:      hashRing,
		subs:          conns,
	}
}

type consistentHashRingPicker struct {
	sync.Mutex
	*zap.SugaredLogger
	hashRing *hashring.HashRing
	subs     ConnPool
}

func (p *consistentHashRingPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	var nodeKey string
	var subConn SubConnWrapper
	// if CtxKey exist, using hashRing to find a node
	if key, ok := info.Ctx.Value(ConsistentHashKey).(string); ok {
		nodeKey, ok = p.hashRing.GetNode(string(key))
		if !ok {
			return balancer.PickResult{}, ErrBalancerHashring
		}
		subConn, ok = p.subs.Get(nodeKey)
		if !ok {
			return balancer.PickResult{}, ErrBalancerHashring
		}
		p.Debugf("consistent-picker: grpc call of %s,hash_key %v, pick %s", info.FullMethodName, key, nodeKey)
		logger.WithTaskID(key).Debugf("consistent-picker: grpc call of %s, hash_key %v, pick %s", info.FullMethodName, key, nodeKey)
	} else {
		subConn, ok = p.subs.GetRandom()
		if !ok {
			return balancer.PickResult{}, ErrBalancerHashring
		}
		p.Warnf("consistent-picker: grpc call of %s, no hash key, random pick %s", info.FullMethodName, nodeKey)
		logger.Warnf("consistent-picker: grpc call of %s, no hash key, random pick %s", info.FullMethodName, nodeKey)
	}

	subConn.Keep()

	if subConn.State() == connectivity.Idle { // connect and wait for ready
		subConn.Connect()
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}
	if subConn.State() == connectivity.Connecting || subConn.State() > connectivity.Ready { // just wait for ready
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	subConn.AddUsingCount(1)
	return balancer.PickResult{
		SubConn: subConn.GetConn(),
		Done: func(info balancer.DoneInfo) {
			subConn.AddUsingCount(-1)
		},
	}, nil
}
