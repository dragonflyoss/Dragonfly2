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
	_ resolver.Builder  = (*D7yResolver)(nil)
	_ resolver.Resolver = (*D7yResolver)(nil)
)

var resolverLogger = grpclog.Component("resolver")

func NewD7yResolver(scheme string, addrs []dfnet.NetAddr) *D7yResolver {
	return &D7yResolver{
		scheme:         scheme,
		bootstrapAddrs: cloneAddresses(addrs),
	}
}

type D7yResolver struct {
	scheme string

	mu              sync.Mutex
	target          resolver.Target
	cc              resolver.ClientConn
	addrs           []dfnet.NetAddr
	bootstrapAddrs  []dfnet.NetAddr
	lastResolverErr error
}

func (r *D7yResolver) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	r.target = target
	r.cc = cc
	if r.bootstrapAddrs != nil {
		if err := r.updateAddrs(r.bootstrapAddrs); err != nil {
			return nil, err
		}
	}
	return r, nil
}

func (r *D7yResolver) Scheme() string {
	return r.scheme
}

// UpdateState calls CC.UpdateState.
func (r *D7yResolver) UpdateState(s resolver.State) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastResolverErr = r.cc.UpdateState(s)
	return r.lastResolverErr
}

func (r *D7yResolver) AddAddresses(addrs []dfnet.NetAddr) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(addrs) == 0 {
		return nil
	}
	rAddrs := r.addrs
	for _, addr := range addrs {
		if !r.containsAddr(addr) {
			rAddrs = append(rAddrs, addr)
		}
	}
	return r.updateAddrs(rAddrs)
}

func (r *D7yResolver) UpdateAddresses(addrs []dfnet.NetAddr) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(addrs) == 0 {
		return nil
	}
	return r.updateAddrs(cloneAddresses(addrs))
}

func (r *D7yResolver) containsAddr(addr dfnet.NetAddr) bool {
	for _, netAddr := range r.addrs {
		if netAddr.GetEndpoint() == addr.GetEndpoint() {
			return true
		}
	}
	return false
}

func (r *D7yResolver) updateAddrs(addrs []dfnet.NetAddr) error {
	if isSameAddresses(addrs, r.addrs) {
		return r.lastResolverErr
	}
	addresses := make([]resolver.Address, len(addrs))
	for i, addr := range addrs {
		addresses[i] = resolver.Address{Addr: addr.GetEndpoint()}
	}
	// update addresses
	r.addrs = addrs

	resolverLogger.Infof("resolver update addresses: %s", addresses)

	r.lastResolverErr = r.cc.UpdateState(resolver.State{Addresses: addresses})
	return r.lastResolverErr
}

func (r *D7yResolver) ResolveNow(resolver.ResolveNowOptions) {}

func (r *D7yResolver) Close() {}

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
			if addr1.GetEndpoint() == addr2.GetEndpoint() {
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
