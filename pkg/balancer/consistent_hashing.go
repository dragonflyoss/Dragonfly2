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

package balancer

import (
	"errors"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/grpclog"
	"stathat.com/c/consistent"
)

type ContextKeyType string

const (
	// BalancerName is the name of consistent-hashing balancer.
	BalancerName = "consistent-hashing"

	// BalancerServiceConfig is a service config that sets the default balancer
	// to the consistent-hashing balancer.
	BalancerServiceConfig = `{"loadBalancingPolicy":"consistent-hashing"}`

	// ContextKey is the key for the grpc request's context.Context which points to
	// the key to hash for the request.
	ContextKey = ContextKeyType("consistent-hashing-key")
)

var logger = grpclog.Component("consistenthashing")

// NewConsistentHashingBuilder creates a new consistent-hashing balancer builder.
func NewConsistentHashingBuilder() balancer.Builder {
	return base.NewBalancerBuilder(
		BalancerName,
		&consistentHashingPickerBuilder{},
		base.Config{HealthCheck: true},
	)
}

type consistentHashingPickerBuilder struct{}

func (b *consistentHashingPickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	logger.Infof("consistentHashingPicker: newPicker called with info: %v", info)
	if len(info.ReadySCs) == 0 {
		return base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}

	// Build hashring and init sub connections map.
	hashring := consistent.New()
	scs := make(map[string]balancer.SubConn, len(info.ReadySCs))
	for sc, scInfo := range info.ReadySCs {
		element := scInfo.Address.Addr + scInfo.Address.ServerName
		hashring.Add(element)
		scs[element] = sc
	}

	return &consistentHashingPicker{
		subConns: scs,
		hashring: hashring,
	}
}

type consistentHashingPicker struct {
	subConns map[string]balancer.SubConn
	hashring *consistent.Consistent
}

func (p *consistentHashingPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	taskID, ok := info.Ctx.Value(ContextKey).(string)
	if !ok {
		return balancer.PickResult{}, errors.New("picker can not found task id")
	}

	element, err := p.hashring.Get(taskID)
	if err != nil {
		return balancer.PickResult{}, err
	}
	logger.Infof("task %s picks connection %s", taskID, element)

	return balancer.PickResult{
		SubConn: p.subConns[element],
	}, nil
}
