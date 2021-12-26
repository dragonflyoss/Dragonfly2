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
	"google.golang.org/grpc/resolver"

	"d7y.io/dragonfly/v2/internal/dfnet"
)

var (
	_ resolver.Builder  = (*d7yResolverBuilder)(nil)
	_ resolver.Resolver = (*d7yResolver)(nil)
)

func NewD7yResolverBuilder(scheme string) resolver.Builder {
	return &d7yResolverBuilder{scheme: scheme}
}

type d7yResolverBuilder struct {
	scheme string
}

func (b *d7yResolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	var err error
	r := &d7yResolver{
		scheme: b.scheme,
		target: target,
		cc:     cc,
		addrs:  nil,
	}
	if len(r.addrs) != 0 {
		err = r.updateAddrs(r.addrs)
	}
	return r, err
}

func (b *d7yResolverBuilder) Scheme() string {
	return b.scheme
}

type d7yResolver struct {
	scheme  string
	target  resolver.Target
	cc      resolver.ClientConn
	watcher *Watcher
	addrs   []dfnet.NetAddr
}

type Watcher struct {
}

//func (r *d7yResolver) UpdateAddrs(addrs []dfnet.NetAddr) error {
//	if len(addrs) == 0 {
//		return nil
//	}
//
//	updateFlag := false
//	if len(addrs) != len(r.addrs) {
//		updateFlag = true
//	} else {
//		for i := 0; i < len(addrs); i++ {
//			if addrs[i] != r.addrs[i] {
//				updateFlag = true
//				break
//			}
//		}
//	}
//
//	if !updateFlag {
//		return nil
//	}
//
//	return r.updateAddrs(addrs)
//}

//func (r *d7yResolver) updateAddrs(addrs []dfnet.NetAddr) error {
//	addresses := make([]resolver.Address, len(addrs))
//	for i, addr := range addrs {
//		if addr.Type == dfnet.UNIX {
//			addresses[i] = resolver.Address{Addr: addr.GetEndpoint()}
//		} else {
//			addresses[i] = resolver.Address{Addr: addr.Addr}
//		}
//	}
//	r.addrs = addrs
//
//	log.Printf("resolver update addresses: %v", addresses)
//	if r.cc == nil {
//		return nil
//	}
//	return r.cc.UpdateState(resolver.State{Addresses: addresses})
//}

func (r *d7yResolver) ResolveNow(options resolver.ResolveNowOptions) {

}

func (r *d7yResolver) Close() {}
