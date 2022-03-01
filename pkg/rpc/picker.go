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

	"github.com/distribution/distribution/v3/uuid"
	anypb "github.com/golang/protobuf/ptypes/any"
	"github.com/serialx/hashring"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/status"

	rpcbase "d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/pickreq"
)

var pickerLogger = grpclog.Component("picker")

type d7yPickerBuildInfo struct {
	subConns    map[resolver.Address]balancer.SubConn
	scStates    map[string]connectivity.State
	pickHistory map[string]string
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
	pickHistory map[string]string           // hashKey -> target address
	scStates    map[string]connectivity.State
	hashRing    *hashring.HashRing
}

func (p *d7yHashPicker) Pick(info balancer.PickInfo) (ret balancer.PickResult, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var pickRequest *pickreq.PickRequest
	var targetAddr string
	pickRequest, ok := pickreq.FromContext(info.Ctx)

	defer func() {
		if err != nil {
			pickerLogger.Infof("d7yPicker: failed to pick server for request:%+v, %v", pickRequest, err)
			return
		}
		pickerLogger.Infof("d7yPicker: pick out server: %s for request: %+v", targetAddr, pickRequest)
	}()
	if ok && pickRequest != nil {
		// mark history
		defer func() {
			if err != nil {
				return
			}
			if targetAddr != "" && pickRequest.HashKey != "" {
				p.pickHistory[pickRequest.HashKey] = targetAddr
			}
		}()

		// target address is specified
		if pickRequest.TargetAddr != "" {
			targetAddr = pickRequest.TargetAddr
			if pickRequest.FailedNodes.Has(pickRequest.TargetAddr) {
				err = status.Errorf(codes.Code(rpcbase.Code_ServerUnavailable), "targetAddr %s is in failedNodes: %s", pickRequest.TargetAddr,
					pickRequest.FailedNodes)
				return
			}
			sc, ok := p.subConns[pickRequest.TargetAddr]
			if !ok {
				err = status.Errorf(codes.Code(rpcbase.Code_ServerUnavailable), "cannot find target addr %s", pickRequest.TargetAddr)
				return
			}
			if p.scStates[pickRequest.TargetAddr] == connectivity.TransientFailure {
				// ac.resetTransport() reset scStates to Idle
				err = newUnavailableErr(targetAddr)
				return
			}
			ret.SubConn = sc
			sc.Connect()
			return
		}
		// rpc call is required to stick to hashKey
		if pickRequest.IsStick == true {
			if pickRequest.HashKey == "" {
				err = status.Errorf(codes.Code(rpcbase.Code_ServerUnavailable), "rpc call is required to stick to hashKey, but hashKey is empty")
				return
			}
			targetAddr, ok = p.pickHistory[pickRequest.HashKey]
			if !ok {
				err = status.Errorf(codes.Code(rpcbase.Code_ServerUnavailable), "rpc call is required to stick to hashKey %s, but cannot find it in pick history", pickRequest.HashKey)
				return
			}
			if pickRequest.FailedNodes.Has(targetAddr) {
				err = status.Errorf(codes.Code(rpcbase.Code_ServerUnavailable), "stick targetAddr %s is in failedNodes: %s", targetAddr, pickRequest.FailedNodes)
				return
			}
			sc, ok := p.subConns[targetAddr]
			if !ok {
				err = status.Errorf(codes.Code(rpcbase.Code_ServerUnavailable), "cannot find target addr %s", targetAddr)
				return
			}

			if p.scStates[targetAddr] == connectivity.TransientFailure {
				// ac.resetTransport() reset scStates to Idle
				// TODO consider change p.scStates[targetAddr] for retry
				err = newUnavailableErr(targetAddr)
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
			err = status.Errorf(codes.Code(rpcbase.Code_ServerUnavailable), "failed to get available target nodes")
			return
		}
		for _, addr := range targetAddrs {
			if !pickRequest.FailedNodes.Has(addr) {
				targetAddr = addr
				break
			}
		}
		if targetAddr == "" {
			err = status.Errorf(codes.Code(rpcbase.Code_ServerUnavailable), "all server nodes have tried but failed")
			return
		}
		if p.scStates[targetAddr] == connectivity.TransientFailure {
			// ac.resetTransport() reset scStates to Idle
			err = newUnavailableErr(targetAddr)
			return
		}
		ret.SubConn = p.subConns[targetAddr]
		ret.SubConn.Connect()
		return
	}
	// pickRequest is not specified, select a node random and no need mark history
	key := uuid.Generate().String()
	targetAddr, ok = p.hashRing.GetNode(key)
	if !ok {
		err = status.Errorf(codes.Code(rpcbase.Code_ServerUnavailable), "failed to get available target nodes")
		return
	}
	if p.scStates[targetAddr] == connectivity.TransientFailure {
		// ac.resetTransport() reset scStates to Idle
		err = newUnavailableErr(targetAddr)
		return
	}
	ret.SubConn = p.subConns[targetAddr]
	ret.SubConn.Connect()
	return
}

func newUnavailableErr(targetAddr string) error {
	var err error
	st := status.Newf(codes.Unavailable, "can not establish connection to %s", targetAddr)
	st, err = st.WithDetails(&anypb.Any{Value: []byte(targetAddr)})
	if err != nil {
		return err
	}
	return st.Err()
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
