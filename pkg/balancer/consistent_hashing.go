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
	"fmt"
	"reflect"

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

// searchCircleLimit is the limit of searching circle.
const searchCircleLimit = 10

var logger = grpclog.Component("consistenthashing")

// NewConsistentHashingBuilder creates a new consistent-hashing balancer builder.
func NewConsistentHashingBuilder() (balancer.Builder, *ConsistentHashingPickerBuilder) {
	pickerBuilder := &ConsistentHashingPickerBuilder{}
	return base.NewBalancerBuilder(
		BalancerName,
		pickerBuilder,
		base.Config{HealthCheck: true},
	), pickerBuilder
}

type ConsistentHashingPickerBuilder struct {
	hashring *consistent.Consistent
	members  []string
	circle   map[string]string
}

func (b *ConsistentHashingPickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	logger.Infof("consistentHashingPicker: newPicker called with info: %v", info)
	if len(info.ReadySCs) == 0 {
		return base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}

	// Build hashring and init sub connections map.
	b.hashring = consistent.New()
	scs := make(map[string]balancer.SubConn, len(info.ReadySCs))
	for sc, scInfo := range info.ReadySCs {
		element := fmt.Sprintf("%s:%s", scInfo.Address.Addr, scInfo.Address.ServerName)
		b.hashring.Add(element)
		scs[element] = sc
	}

	return &consistentHashingPicker{
		subConns: scs,
		hashring: b.hashring,
	}
}

func (b *ConsistentHashingPickerBuilder) GetCircle() (map[string]string, error) {
	if b.hashring == nil {
		return nil, errors.New("invalid hashring")
	}

	members := b.hashring.Members()
	if reflect.DeepEqual(b.members, members) {
		return b.circle, nil
	}

	circle := make(map[string]string, len(members))
	for i := 0; i <= len(members)*searchCircleLimit; i++ {
		key := fmt.Sprint(i)
		member, err := b.hashring.Get(key)
		if err != nil {
			logger.Errorf("hashring get member failed: %s", err.Error())
			continue
		}

		if _, ok := circle[member]; !ok {
			circle[member] = key
			if len(circle) == len(members) {
				b.members = members
				b.circle = circle
				return circle, nil
			}
		}
	}

	return nil, errors.New("can not generate circle")
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
