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
	"strconv"
	"sync"

	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/distribution/distribution/v3/uuid"
	"github.com/serialx/hashring"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/status"
)

// PickRequest contains the clues to pick subConn
type PickRequest struct {
	HashKey     string
	FailedNodes sets.String
	IsStick     bool
	TargetAddr  string
}

type pickKey struct{}

// NewContext creates a new context with pick clues attached.
func NewContext(ctx context.Context, p *PickRequest) context.Context {
	return context.WithValue(ctx, pickKey{}, p)
}

// FromContext returns the pickReq clues in ctx if it exists.
func FromContext(ctx context.Context) (p *PickRequest, ok bool) {
	p, ok = ctx.Value(pickKey{}).(*PickRequest)
	return
}

type d7yPickerBuildInfo struct {
	subConns    map[resolver.Address]balancer.SubConn
	scStates    map[balancer.SubConn]connectivity.State
	pickHistory map[string]balancer.SubConn
}

func newD7yPicker(info d7yPickerBuildInfo) balancer.Picker {
	if len(info.subConns) == 0 {
		return base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}
	weights := make(map[string]int, len(info.subConns))
	subConns := make(map[string]balancer.SubConn)
	for addr, subConn := range info.subConns {
		weights[addr.Addr] = getWeight(addr)
		subConns[addr.Addr] = subConn
	}
	return &d7yHashPicker{
		subConns:    subConns,
		pickHistory: info.pickHistory,
		scStates:    info.scStates,
		hashRing:    hashring.NewWithWeights(weights),
	}
}

var (
	_ balancer.Picker = (*d7yHashPicker)(nil)
)

type d7yHashPicker struct {
	mu          sync.Mutex
	subConns    map[string]balancer.SubConn // target address string -> balancer.SubConn
	pickHistory map[string]balancer.SubConn
	scStates    map[balancer.SubConn]connectivity.State
	hashRing    *hashring.HashRing
}

func (p *d7yHashPicker) Pick(info balancer.PickInfo) (ret balancer.PickResult, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	var pickRequest *PickRequest
	pickRequest, ok := FromContext(info.Ctx)
	if ok && pickRequest != nil {
		// target address is specified
		if pickRequest.TargetAddr != "" {
			sc, ok := p.subConns[pickRequest.TargetAddr]
			if !ok {
				err = status.Errorf(codes.FailedPrecondition, "cannot find target addr %s", pickRequest.TargetAddr)
				return
			}
			ret.SubConn = sc
			sc.Connect()
			return
		}
		// rpc call is required to stick to hashKey
		if pickRequest.IsStick == true {
			if pickRequest.HashKey == "" {
				err = status.Errorf(codes.FailedPrecondition, "rpc call is required to stick to hashKey, but hashKey is empty")
				return
			}
			sc, ok := p.pickHistory[pickRequest.HashKey]
			if !ok {
				err = status.Errorf(codes.FailedPrecondition, "rpc call is required to stick to hashKey %s, but cannot find it in pick history", pickRequest.HashKey)
				return
			}
			ret.SubConn = sc
			sc.Connect()
			return
		}
		// not require stick
		key := uuid.Generate().String()
		if pickRequest.HashKey != "" {
			key = pickRequest.HashKey
		}
		targetAddrs, ok := p.hashRing.GetNodes(key, p.hashRing.Size())
		if !ok {
			err = status.Errorf(codes.FailedPrecondition, "failed to get available target nodes")
			return
		}
		var targetAddr string
		for _, addr := range targetAddrs {
			if !pickRequest.FailedNodes.Has(addr) {
				targetAddr = addr
				break
			}
		}
		if targetAddr == "" {
			err = status.Errorf(codes.FailedPrecondition, "all server nodes have tried but failed")
			return
		}
		// mark history
		if pickRequest.HashKey != "" {
			ret.SubConn = p.subConns[targetAddr]
			p.pickHistory[key] = ret.SubConn
			ret.SubConn.Connect()
			return
		}
	}
	// pickRequest is not specified, select a node random and no need mark history
	key := uuid.Generate().String()
	targetAddr, ok := p.hashRing.GetNode(key)
	if !ok {
		err = status.Errorf(codes.FailedPrecondition, "failed to get available target nodes")
		return
	}
	ret.SubConn = p.subConns[targetAddr]
	ret.SubConn.Connect()
	return
}

const (
	WeightKey = "weight"
)

func getWeight(addr resolver.Address) int {
	if addr.Attributes == nil {
		return 1
	}
	value := addr.Attributes.Value(WeightKey)
	if value == nil {
		return 1
	}
	weight, err := strconv.Atoi(value.(string))
	if err == nil {
		return weight
	}
	return 1
}
