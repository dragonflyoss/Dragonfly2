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
	"log"

	"go.uber.org/atomic"

	"google.golang.org/grpc/resolver"

	"d7y.io/dragonfly/v2/internal/dfnet"
)

const (
	CDNScheme       = "cdn"
	SchedulerScheme = "scheduler"
	DaemonScheme    = "dfdaemon"
)

var (
	_ resolver.Builder  = &D7yResolver{}
	_ resolver.Resolver = &D7yResolver{}
)

func NewD7yResolver(scheme string, addrs []dfnet.NetAddr) *D7yResolver {
	return &D7yResolver{scheme: scheme, addrs: addrs}
}

type D7yResolver struct {
	built  atomic.Bool
	scheme string
	target resolver.Target
	cc     resolver.ClientConn
	addrs  []dfnet.NetAddr
}

func (r *D7yResolver) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	var err error
	r.target = target
	r.cc = cc
	r.built.Store(true)
	if len(r.addrs) != 0 {
		err = r.UpdateAddrs(r.addrs)
	}
	return r, err
}

func (r *D7yResolver) Scheme() string {
	return r.scheme
}

func (r *D7yResolver) UpdateAddrs(addrs []dfnet.NetAddr) error {
	if len(addrs) == 0 {
		return nil
	}

	updateFlag := false
	if len(addrs) != len(r.addrs) {
		updateFlag = true
	} else {
		for i := 0; i < len(addrs); i++ {
			if addrs[i] != r.addrs[i] {
				updateFlag = true
				break
			}
		}
	}

	if !updateFlag {
		return nil
	}

	addresses := make([]resolver.Address, len(addrs))
	for i, addr := range addrs {
		if addr.Type == dfnet.UNIX {
			addresses[i] = resolver.Address{Addr: addr.GetEndpoint()}
		} else {
			addresses[i] = resolver.Address{Addr: addr.Addr}
		}
	}
	r.addrs = addrs

	log.Printf("resolver update addresses: %v", addresses)
	if r.built.Load() == false {
		return nil
	}
	return r.cc.UpdateState(resolver.State{Addresses: addresses})
}

func (r *D7yResolver) ResolveNow(options resolver.ResolveNowOptions) {}

func (r *D7yResolver) Close() {}
