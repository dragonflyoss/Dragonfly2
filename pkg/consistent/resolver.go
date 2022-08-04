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

// RegisterResolver register the dragonfly resovler builder to the grpc with custom schema
func RegisterResolver[T any](dynConfig DynConfig[T]) {
	resolver.Register(&DragonflyResolverBuilder[T]{
		dynConfig: dynConfig,
	})
}

// DragonflyResolverBuilder implement resolver.Builder
// T is the dynConfig structure
type DragonflyResolverBuilder[T any] struct {
	dynConfig DynConfig[T]
}

func (b *DragonflyResolverBuilder[T]) Scheme() string {
	return DragonflyScheme
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
		dynConfig:     b.dynConfig,
		cc:            cc,
		rn:            make(chan T, 1),
	}

	// register as dynConfig observer
	b.dynConfig.Register(r)

	go func() {
		r.watch()
	}()
	r.ResolveNow(resolver.ResolveNowOptions{})
	return r, nil
}

type dResolver[T any] struct {
	*zap.SugaredLogger
	ctx    context.Context
	cancel context.CancelFunc
	// convert is used to convert dnyConfig data to target addresses
	//convert   func(data T) []string
	//reload    func() (T, error)
	dynConfig DynConfig[T]
	host      string
	cc        resolver.ClientConn
	// rn channel is used by ResolveNow() to force an immediate resolution of the target.
	// or used by Observer to change target
	rn chan T
}

// resolve will update current ClientConn addresses
func (b *dResolver[T]) resolve(data T) {
	targets := b.dynConfig.Convert(b.host, data)
	b.Debugf("resolver update addrs:%v", targets)
	if len(targets) == 0 {
		b.Warn("resolve empty address")
	}
	addresses := make([]resolver.Address, 0, len(targets))
	for _, t := range targets {
		addresses = append(addresses, resolver.Address{Addr: t})
	}

	err := b.cc.UpdateState(resolver.State{
		Addresses: addresses,
	})
	if err != nil {
		b.Errorf("resolver update ClientConn error %v", err)
	}
}

// ResolveNow will be called by gRPC to try to resolve the target name again.
// gRPC will trigger resolveNow everytime SubConn connect fail. But we only need
// to refresh addresses from manager when all SubConn fail.
// So here we don't trigger resolving to reduce the pressure of manager
func (b *dResolver[T]) ResolveNow(resolver.ResolveNowOptions) {
	b.Info("resolveNow is called")
	//data, err := b.dynConfig.Reload()
	//if err != nil {
	//	b.Errorf("resolver reload error:%v", err)
	//	return
	//}
	//b.rn <- data
}

func (b *dResolver[T]) OnNotify(data T) {
	select {
	case b.rn <- data:
	default:
	}
}

// Close closes the resolver.
func (b *dResolver[T]) Close() {
	b.dynConfig.Deregister(b)
	b.cancel()
}

func (b *dResolver[T]) watch() {
	for {
		select {
		case <-b.ctx.Done():
			return
		case data := <-b.rn:
			b.resolve(data)
		}
	}
}
