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

package resolver

import (
	"fmt"
	"reflect"
	"sync"

	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/resolver"

	"d7y.io/dragonfly/v2/scheduler/config"
)

const (
	// SeedPeerScheme is the seed peer schema used by resolver.
	SeedPeerScheme = "sp"
)

var (
	// SeedPeerVirtualTarget is seed peer virtual target.
	SeedPeerVirtualTarget = fmt.Sprintf("%s://localhost", SeedPeerScheme)
)

var slogger = grpclog.Component("seed_peer_resolver")

// SeedPeerResolver implement resolver.Builder.
type SeedPeerResolver struct {
	addrs     []resolver.Address
	cc        resolver.ClientConn
	dynconfig config.DynconfigInterface
	mu        *sync.Mutex
}

// RegisterSeedPeer register the dragonfly resovler builder to the grpc with custom schema.
func RegisterSeedPeer(dynconfig config.DynconfigInterface) {
	resolver.Register(&SeedPeerResolver{dynconfig: dynconfig, mu: &sync.Mutex{}})
}

// Scheme returns the resolver scheme.
func (r *SeedPeerResolver) Scheme() string {
	return SeedPeerScheme
}

// Build creates a new resolver for the given target.
//
// gRPC dial calls Build synchronously, and fails if the returned error is
// not nil.
func (r *SeedPeerResolver) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	r.cc = cc
	r.dynconfig.Register(r)
	r.ResolveNow(resolver.ResolveNowOptions{})
	return r, nil
}

// ResolveNow will be called by gRPC to try to resolve the target name again.
// gRPC will trigger resolveNow everytime SubConn connect fail. But we only need
// to refresh addresses from manager when all SubConn fail.
// So here we don't trigger resolving to reduce the pressure of manager.
func (r *SeedPeerResolver) ResolveNow(resolver.ResolveNowOptions) {
	// Avoid concurrent GetResolveSeedPeerAddrs calls.
	if !r.mu.TryLock() {
		slogger.Warning("resolve addresses is running")
		return
	}
	defer r.mu.Unlock()

	addrs, err := r.dynconfig.GetResolveSeedPeerAddrs()
	if err != nil {
		slogger.Errorf("resolve addresses error %v", err)
		return
	}

	if reflect.DeepEqual(r.addrs, addrs) {
		return
	}
	r.addrs = addrs

	if err := r.cc.UpdateState(resolver.State{
		Addresses: addrs,
	}); err != nil {
		slogger.Errorf("resolver update ClientConn error %v", err)
	}

	slogger.Infof("resolve addresses %v", addrs)
}

// Close closes the resolver.
func (r *SeedPeerResolver) Close() {
	r.dynconfig.Deregister(r)
}

// OnNotify is triggered and resolver address is updated.
func (r *SeedPeerResolver) OnNotify(data *config.DynconfigData) {
	r.ResolveNow(resolver.ResolveNowOptions{})
}
