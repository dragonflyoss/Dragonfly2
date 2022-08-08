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

package consistent

import (
	"context"

	"go.uber.org/zap"
	"google.golang.org/grpc/resolver"

	logger "d7y.io/dragonfly/v2/internal/dflog"
)

const (
	DragonflyScheme        = "d7y"
	DragonflyHostScheduler = "scheduler"
)

// DragonflyResolverBuilder implement resolver.Builder
// T is the dynConfig structure.
type DragonflyResolverBuilder[T any] struct {
	dynconfig Dynconfig[T]
}

// RegisterResolver register the dragonfly resovler builder to the grpc with custom schema.
func RegisterResolver[T any](dynconfig Dynconfig[T]) {
	resolver.Register(&DragonflyResolverBuilder[T]{
		dynconfig: dynconfig,
	})
}

func (b *DragonflyResolverBuilder[T]) Scheme() string {
	return DragonflyScheme
}

type dResolver[T any] struct {
	*zap.SugaredLogger
	ctx       context.Context
	cancel    context.CancelFunc
	dynconfig Dynconfig[T]
	host      string
	cc        resolver.ClientConn
}

// Build creates a new resolver for the given target.
//
// gRPC dial calls Build synchronously, and fails if the returned error is
// not nil.
func (b *DragonflyResolverBuilder[T]) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	ctx, cancel := context.WithCancel(context.Background())
	r := &dResolver[T]{
		SugaredLogger: logger.GrpcLogger.With("component", "resolver", "target", target.URL.String()),
		ctx:           ctx,
		cancel:        cancel,
		host:          target.URL.Host,
		dynconfig:     b.dynconfig,
		cc:            cc,
	}

	b.dynconfig.Register(r)
	return r, nil
}

// ResolveNow will be called by gRPC to try to resolve the target name again.
// gRPC will trigger resolveNow everytime SubConn connect fail. But we only need
// to refresh addresses from manager when all SubConn fail.
// So here we don't trigger resolving to reduce the pressure of manager.
func (b *dResolver[T]) ResolveNow(resolver.ResolveNowOptions) {
	b.Info("resolveNow is called")
}

// OnNotify is triggered and resolver address is updated.
func (b *dResolver[T]) OnNotify(data T) {
	targets, err := b.dynconfig.GetResolverAddrs()
	if err != nil {
		b.Errorf("get resolver addresses error %v", err)
		return
	}

	b.Debugf("resolver update addrs:%v", targets)
	if len(targets) == 0 {
		b.Warn("resolve empty address")
	}

	addrs := make([]resolver.Address, 0, len(targets))
	for _, target := range targets {
		addrs = append(addrs, target)
	}

	if err := b.cc.UpdateState(resolver.State{
		Addresses: addrs,
	}); err != nil {
		b.Errorf("resolver update ClientConn error %v", err)
	}
}

// Close closes the resolver.
func (b *dResolver[T]) Close() {
	b.dynconfig.Deregister(b)
	b.cancel()
}
