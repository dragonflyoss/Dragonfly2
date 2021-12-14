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

	"google.golang.org/grpc/resolver"

	"d7y.io/dragonfly/v2/internal/dfnet"
)

const (
	CDNScheme       = "cdn"
	SchedulerScheme = "scheduler"
	DaemonScheme    = "dfdaemon"
)

var (
	_ resolver.Builder  = &d7yResolver{}
	_ resolver.Resolver = &d7yResolver{}
)

type d7yResolver struct {
	scheme string
	target resolver.Target
	cc     resolver.ClientConn
	addrs  []dfnet.NetAddr
}

func (r *d7yResolver) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	r.target = target
	r.cc = cc
	return r, nil
}

func (r *d7yResolver) Scheme() string {
	return r.scheme
}

func (r *d7yResolver) UpdateAddrs(addrs []dfnet.NetAddr) error {
	if len(addrs) == 0 {
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
	if r.cc == nil {
		return nil
	}
	log.Printf("resolver update addresses: %v", addresses)
	return r.cc.UpdateState(resolver.State{Addresses: addresses})
}

func (r *d7yResolver) ResolveNow(options resolver.ResolveNowOptions) {}

func (r *d7yResolver) Close() {}
