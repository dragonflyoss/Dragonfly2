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
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"sync"
	"time"

	"d7y.io/dragonfly/v2/pkg/rpc/manager"
	"github.com/patrickmn/go-cache"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/resolver"
)

var (
	_ resolver.Builder  = (*d7yResolverBuilder)(nil)
	_ resolver.Resolver = (*d7yResolver)(nil)
)

var resolverLogger = grpclog.Component("resolver")

func NewD7yResolverBuilder(scheme string) resolver.Builder {
	return &d7yResolverBuilder{scheme: scheme}
}

type d7yResolverBuilder struct {
	scheme string
	config Config
}

func (b *d7yResolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	clientConn, err := grpc.Dial(target.URL.String())
	if err != nil {
		return nil, err
	}
	watcher := newWatcher(clientConn, WatchConfig{})
	r := &d7yResolver{
		scheme:  b.scheme,
		target:  target,
		cc:      cc,
		watcher: watcher,
	}
	r.wg.Add(1)
	go r.start()
	return r, nil
}

func (b *d7yResolverBuilder) Scheme() string {
	return b.scheme
}

type d7yResolver struct {
	scheme  string
	target  resolver.Target
	cc      resolver.ClientConn
	watcher *Watcher
	wg      sync.WaitGroup
}

func (r *d7yResolver) start() {
	defer r.wg.Done()
	out, err := r.watcher.Watch()
	if err != nil {
		r.cc.ReportError(err)
	} else {
		for addr := range out {
			r.cc.UpdateState(resolver.State{Addresses: addr})
		}
	}
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

func (r *d7yResolver) ResolveNow(resolver.ResolveNowOptions) {
	select {
	case r.watcher.rn <- struct{}{}:
	default:
	}
}

func (r *d7yResolver) Close() {
	r.watcher.Close()
	r.wg.Wait()
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

func (w *Watcher) Watch() (chan []resolver.Address, error) {
	out := make(chan []resolver.Address, 1)
	addrs, err := w.getAllAddresses()
	if err != nil {
		return nil, err
	}
	w.lastAddrs = addrs
	out <- w.cloneAddresses(w.lastAddrs)
	w.wg.Add(1)
	go func() {
		defer func() {
			w.wg.Done()
			close(out)
		}()
		timer := time.NewTicker(w.config.interval)
		for {
			select {
			case <-timer.C:
				addrs, err := w.getAllAddresses()
				if err != nil {
					logger.Errorf("failed to get address: %v", err)
					continue
				}
				if !isSameAddresses(addrs, w.lastAddrs) {
					w.lastAddrs = addrs
					out <- w.cloneAddresses(addrs)
				}
			case <-w.rn:
				addrs, err := w.getAllAddresses()
				if err != nil {
					logger.Errorf("failed to get address: %v", err)
					continue
				}
				if !isSameAddresses(addrs, w.lastAddrs) {
					w.lastAddrs = addrs
					out <- w.cloneAddresses(addrs)
				}
			case <-w.ctx.Done():
				timer.Stop()
				return
			}
		}
	}()
	return out, nil
}

func (w *Watcher) cloneAddresses(in []resolver.Address) []resolver.Address {
	out := make([]resolver.Address, len(in))
	for i := 0; i < len(in); i++ {
		out[i] = in[i]
	}
	return out
}

func isSameAddresses(addresses1, addresses2 []resolver.Address) bool {
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

func (w *Watcher) getAllAddresses() ([]resolver.Address, error) {
	var addresses []resolver.Address
	resp, err := manager.NewManagerClient(w.conn).ListSchedulers(context.Background(), &manager.ListSchedulersRequest{
		SourceType: 0,
		HostName:   "",
		Ip:         "",
		HostInfo:   nil,
	})
	if err == nil {
		for _, scheduler := range resp.Schedulers {
			addresses = append(addresses, resolver.Address{
				Addr:               scheduler.Ip,
				ServerName:         "",
				Attributes:         nil,
				BalancerAttributes: nil,
			})
		}
		w.cache.Set("servers", addresses, cache.NoExpiration)
		if err := w.cache.SaveFile(w.config.path); err != nil {

		}
		return addresses, nil
	}
	servers, ok := w.cache.Get("servers")
	if ok {
		return servers.([]resolver.Address), nil
	}

	return addresses, nil
}

func (w *Watcher) Close() {
	w.cancel()
	w.conn.Close()
	w.wg.Wait()
}
