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
	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	"google.golang.org/grpc/resolver"
)

const (
	CDNScheme       = "cdn"
	SchedulerScheme = "scheduler"
	ManagerScheme   = "manager"
)

var (
	_ resolver.Builder  = &d7yResolverBuilder{}
	_ resolver.Resolver = &d7yResolver{}

	Scheme2Resolver = map[string]*d7yResolver{}
)

func init() {
	resolver.Register(&d7yResolverBuilder{scheme: CDNScheme})
	resolver.Register(&d7yResolverBuilder{scheme: SchedulerScheme})
	resolver.Register(&d7yResolverBuilder{scheme: ManagerScheme})
}

type d7yResolverBuilder struct {
	scheme string
}

func (builder *d7yResolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	Scheme2Resolver[builder.scheme] = &d7yResolver{
		target: target,
		cc:     cc,
	}
	return Scheme2Resolver[builder.scheme], nil
}

func (builder *d7yResolverBuilder) Scheme() string {
	return builder.scheme
}

// TODO(zzy987) record address, and do not update if addrs are the same when UpdateAddrs is called.
type d7yResolver struct {
	target resolver.Target
	cc     resolver.ClientConn
}

func (r *d7yResolver) UpdateAddrs(addrs []dfnet.NetAddr) error {
	addresses := make([]resolver.Address, len(addrs))
	for i, addr := range addrs {
		addresses[i] = resolver.Address{Addr: addr.GetEndpoint()}
	}
	return r.cc.UpdateState(resolver.State{Addresses: addresses})
}

func (r *d7yResolver) ResolveNow(options resolver.ResolveNowOptions) {}

func (r *d7yResolver) Close() {}
