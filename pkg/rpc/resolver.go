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
	"d7y.io/dragonfly/v2/internal/dfnet"
	"log"
	"sync"
	"time"

	"github.com/patrickmn/go-cache"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/resolver"
)

var (
	_ resolver.Builder  = (*d7yResolver)(nil)
	_ resolver.Resolver = (*d7yResolver)(nil)
)

var resolverLogger = grpclog.Component("resolver")

func NewD7yResolver(scheme string, addrs []dfnet.NetAddr) *d7yResolver {
	return &d7yResolver{scheme: scheme, addrs: addrs}
}

type d7yResolver struct {
	scheme string
	addrs  []dfnet.NetAddr
	config Config
	target resolver.Target
	cc     resolver.ClientConn
	wg     sync.WaitGroup
}

func (r *d7yResolver) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	r.target = target
	r.cc = cc
	if r.addrs != nil {
		r.UpdateState(r.addrs)
	}
	return r, nil
}

func (r *d7yResolver) Scheme() string {
	return r.scheme
}

// UpdateState calls CC.UpdateState.
func (r *d7yResolver) UpdateState(s resolver.State) {
	r.cc.UpdateState(s)
}

func (r *d7yResolver) Add() {

}

func (r *d7yResolver) UpdateAddrs(addrs []dfnet.NetAddr) error {
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

	return r.updateAddrs(addrs)
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

	log.Printf("resolver update addresses: %v", addresses)
	if r.cc == nil {
		return nil
	}
	return r.cc.UpdateState(resolver.State{Addresses: addresses})
}

func (r *d7yResolver) ResolveNow(resolver.ResolveNowOptions) {
}

func (r *d7yResolver) Close() {
}

type Watcher struct {
	config    WatchConfig
	conn      *grpc.ClientConn
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	lastAddrs []resolver.Address
	// rn channel is used by ResolveNow() to force an immediate resolution of the target.
	rn    chan struct{}
	cache *cache.Cache
}

type WatchConfig struct {
	configServer string
	interval     time.Duration
	path         string
}

func newWatcher(clientConn *grpc.ClientConn, config WatchConfig) *Watcher {
	ctx, cancel := context.WithCancel(context.Background())
	w := &Watcher{
		config: config,
		conn:   clientConn,
		ctx:    ctx,
		cancel: cancel,
		cache:  cache.New(cache.DefaultExpiration, time.Minute),
	}
	return w
}
