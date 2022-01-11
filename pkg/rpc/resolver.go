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
	"sync"

	"d7y.io/dragonfly/v2/internal/dfnet"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/resolver"
)

var (
	_ resolver.Builder  = (*d7yResolver)(nil)
	_ resolver.Resolver = (*d7yResolver)(nil)
)

var resolverLogger = grpclog.Component("resolver")

func NewD7yResolver(scheme string, addrs []dfnet.NetAddr) *d7yResolver {
	return &d7yResolver{
		scheme: scheme,
		addrs:  cloneAddresses(addrs),
	}
}

type d7yResolver struct {
	scheme string

	mu     sync.Mutex
	target resolver.Target
	cc     resolver.ClientConn
	addrs  []dfnet.NetAddr
}

func (r *d7yResolver) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	r.target = target
	r.cc = cc
	if r.addrs != nil {
		r.UpdateAddresses(r.addrs)
	}
	return r, nil
}

func (r *d7yResolver) Scheme() string {
	return r.scheme
}

// UpdateState calls CC.UpdateState.
func (r *d7yResolver) UpdateState(s resolver.State) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cc.UpdateState(s)
}

func (r *d7yResolver) AddAddresses(addrs []dfnet.NetAddr) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(addrs) == 0 {
		return
	}

}

func (r *d7yResolver) UpdateAddresses(addrs []dfnet.NetAddr) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(addrs) == 0 {
		return nil
	}
	if isSameAddresses(addrs, r.addrs) {
		return nil
	}

	return r.updateAddrs(cloneAddresses(addrs))
}

func cloneAddresses(in []dfnet.NetAddr) []dfnet.NetAddr {
	out := make([]dfnet.NetAddr, len(in))
	for i := 0; i < len(in); i++ {
		out[i] = in[i]
	}
	return out
}

func isSameAddresses(addresses1, addresses2 []dfnet.NetAddr) bool {
	if len(addresses1) != len(addresses2) {
		return false
	}
	for _, addr1 := range addresses1 {
		found := false
		for _, addr2 := range addresses2 {
			if addr1.Addr == addr2.Addr {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func (r *d7yResolver) updateAddrs(addrs []dfnet.NetAddr) error {
	addresses := make([]resolver.Address, len(addrs))
	for i, addr := range addrs {
		if addr.Type == dfnet.UNIX {
			addresses[i] = resolver.Address{Addr: addr.GetEndpoint()}
		} else {
			addresses[i] = resolver.Address{Addr: addr.Addr}
		}
	}
	r.addrs = addrs

	resolverLogger.Infof("resolver update addresses: %s", addresses)

	return r.cc.UpdateState(resolver.State{Addresses: addresses})
}

func (r *d7yResolver) ResolveNow(resolver.ResolveNowOptions) {
}

func (r *d7yResolver) Close() {
}
