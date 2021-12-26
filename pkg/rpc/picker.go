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
	"google.golang.org/grpc/balancer/base"
	"log"
	"time"

	"github.com/serialx/hashring"
	"google.golang.org/grpc/balancer"

	"d7y.io/dragonfly/v2/internal/dferrors"
)

// PickKey is a context.Context Value key. Its associated value should be a *PickReq.
type PickKey struct{}

// PickReq is a context.Context Value.
type PickReq struct {
	Key     string
	Attempt int
}

var (
	_ balancer.Picker = (*d7yHashPicker)(nil)
)

type pickResult struct {
	ctx        context.Context
	key        string
	targetAddr string
	sc         balancer.SubConn
	pickTime   time.Time
}

func newD7yHashPicker(subConns map[string]balancer.SubConn, pickHistory map[string]balancer.SubConn) balancer.Picker {
	if len(subConns) == 0 {
		return base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}
	addrs := make([]string, 0)
	for addr := range subConns {
		addrs = append(addrs, addr)
	}
	return &d7yHashPicker{
		subConns:    subConns,
		hashRing:    hashring.New(addrs),
		pickHistory: pickHistory,
	}
}

type d7yHashPicker struct {
	subConns    map[string]balancer.SubConn // address string -> balancer.SubConn
	hashRing    *hashring.HashRing
	reportChan  chan<- pickResult
	pickHistory map[string]balancer.SubConn
}

func (p *d7yHashPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	var (
		targetAddr string
		ret        balancer.PickResult
	)
	pickReq, ok := info.Ctx.Value(PickKey{}).(*PickReq)
	if !ok {
		pickReq = &PickReq{
			Key:     info.FullMethodName,
			Attempt: 1,
		}
	}
	log.Printf("pick for %s, for %d time(s)\n", pickReq.Key, pickReq.Attempt)

	if pickReq.Attempt == 1 {
		if v, ok := p.pickHistory.Load(pickReq.Key); ok {
			targetAddr = v.(string)
			ret.SubConn = p.subConns[targetAddr]
			//p.reportChan <- PickResult{Key: pickReq.Key, TargetAddr: targetAddr, SC: ret.SubConn, Ctx: info.Ctx, PickTime: time.Now()}
		} else if targetAddr, ok = p.hashRing.GetNode(pickReq.Key); ok {
			ret.SubConn = p.subConns[targetAddr]
			//p.reportChan <- PickResult{Key: pickReq.Key, TargetAddr: targetAddr, SC: ret.SubConn, Ctx: info.Ctx, PickTime: time.Now()}
		}
	} else {
		if targetAddrs, ok := p.hashRing.GetNodes(pickReq.Key, pickReq.Attempt); ok {
			targetAddr = targetAddrs[pickReq.Attempt-1]
			ret.SubConn = p.subConns[targetAddr]
			//p.reportChan <- PickResult{Key: pickReq.Key, TargetAddr: targetAddr, SC: ret.SubConn, Ctx: info.Ctx, PickTime: time.Now()}
		}
	}

	if ret.SubConn == nil {
		//return ret, balancer.ErrNoSubConnAvailable
		return ret, dferrors.ErrNoCandidateNode
	}
	return ret, nil
}
