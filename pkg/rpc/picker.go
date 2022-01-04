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

var (
	_ balancer.Picker = (*d7yHashPicker)(nil)
)

// PickRequest contains the information of the subConn pick info
type PickRequest struct {
	HashKey     string
	FailedNodes sets.String
	IsStick     bool
	TargetAddr  string
}

type pickKey struct{}

// NewContext creates a new context with pick information attached.
func NewContext(ctx context.Context, p *PickRequest) context.Context {
	return context.WithValue(ctx, pickKey{}, p)
}

// FromContext returns the pickReq information in ctx if it exists.
func FromContext(ctx context.Context) (p *PickRequest, ok bool) {
	p, ok = ctx.Value(pickKey{}).(*PickRequest)
	return
}

type D7yContextKey string

type D7yContextValue struct {
}

type PickerReq struct {
	HashKey string
	Attempt int
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
		subConns: subConns,
		balancer: info.balancer,
		scStates: info.scStates,
		hashRing: hashring.NewWithWeights(weights),
	}
}

type d7yPickerBuildInfo struct {
	subConns map[resolver.Address]balancer.SubConn
	balancer *d7yBalancer
	scStates map[balancer.SubConn]connectivity.State
}

type d7yHashPicker struct {
	mu       sync.Mutex
	subConns map[string]balancer.SubConn // target address string -> balancer.SubConn
	balancer *d7yBalancer
	scStates map[balancer.SubConn]connectivity.State
	hashRing *hashring.HashRing
}

func (p *d7yHashPicker) Pick(info balancer.PickInfo) (ret balancer.PickResult, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	var pickRequest *PickRequest
	pickRequest, ok := FromContext(info.Ctx)
	if ok {
		// target address is specified
		if pickRequest.TargetAddr != "" {
			subConn, ok := p.subConns[pickRequest.TargetAddr]
			if !ok {
				err = status.Errorf(codes.FailedPrecondition, "cannot find target addr %s", pickRequest.TargetAddr)
				return
			}
			ret.SubConn = subConn
			subConn.Connect()
			return
		}
		// rpc call is required to stick to hashKey
		if pickRequest.IsStick == true {
			if pickRequest.HashKey == "" {
				err = status.Errorf(codes.FailedPrecondition, "rpc call is required to stick to hashKey, but hashKey is empty")
				return
			}
			subConn, ok := p.balancer.pickHistory[pickRequest.HashKey]
			if !ok {
				err = status.Errorf(codes.FailedPrecondition, "rpc call is required to stick to hashKey %s, but cannot found history")
				return
			}
			ret.SubConn = subConn
			subConn.Connect()
			return ret, nil
		}
	}
	key := uuid.Generate().String()
	if pickRequest != nil && pickRequest.HashKey != "" {
		key = pickRequest.HashKey
	}
	targetAddrs, ok := p.hashRing.GetNodes(key, p.hashRing.Size())
	if !ok {
		err = status.Errorf(codes.FailedPrecondition, "failed to get available target nodes")
		return
	}
	var targetAddress string
	for _, targetAddr := range targetAddrs {
		if pickRequest != nil && !pickRequest.FailedNodes.Has(targetAddr) {
			targetAddress = targetAddr
			break
		}
	}
	if targetAddress == "" {
		err = status.Errorf(codes.FailedPrecondition, "all server nodes have tried")
	}
	if pickRequest != nil && pickRequest.HashKey != "" {
		ret.SubConn = p.subConns[targetAddress]
		p.balancer.pickHistory[key] = p.subConns[targetAddress]
		ret.SubConn.Connect()
	}
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
