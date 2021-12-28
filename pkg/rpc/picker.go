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
	"strconv"
	"sync"

	"github.com/serialx/hashring"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/resolver"

	"d7y.io/dragonfly/v2/internal/dferrors"
)

var (
	_ balancer.Picker = (*d7yHashPicker)(nil)
)

const (
	StickFlag = "stick"
	HashKey   = "d7yHashKey"
)

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
		stickFlag:   StickFlag,
		hashKey:     HashKey,
	}
}

type d7yPickerBuildInfo struct {
	subConns    map[resolver.Address]balancer.SubConn
	pickHistory map[string]balancer.SubConn
	scStates    map[balancer.SubConn]connectivity.State
}

type d7yHashPicker struct {
	mu          sync.Mutex
	subConns    map[string]balancer.SubConn // target address string -> balancer.SubConn
	pickHistory map[string]balancer.SubConn // hashKey -> balancer.SubConn
	scStates    map[balancer.SubConn]connectivity.State
	hashRing    *hashring.HashRing
	stickFlag   string
	hashKey     string
}

func (p *d7yHashPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	var ret balancer.PickResult
	var isStick = false
	if stick := info.Ctx.Value(p.stickFlag); stick != nil {
		isStick = stick.(bool)
	}

	key := info.Ctx.Value(p.hashKey).(string)
	if isStick {
		subConn, ok := p.pickHistory[key]
		if !ok {

		}
		ret.SubConn = subConn
		return ret, nil
	}

	targetAddr, ok := p.hashRing.GetNode(key)
	if ok {
		ret.SubConn = p.subConns[targetAddr]
		p.pickHistory[key] = p.subConns[targetAddr]
		return ret, nil
	}

	if ret.SubConn == nil {
		//return ret, balancer.ErrNoSubConnAvailable
		return ret, dferrors.ErrNoCandidateNode
	}
	return ret, nil
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
