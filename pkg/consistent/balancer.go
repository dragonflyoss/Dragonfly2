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
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/connectivity"

	logger "d7y.io/dragonfly/v2/internal/dflog"
)

const (
	// BalancerName is the name of consistent-hashring balancer.
	BalancerName = "consistent-hashring"

	// BalancerServiceConfig is a service config that sets the default balancer
	// to the consistent-hashring balancer
	BalancerServiceConfig = `{"loadBalancingPolicy":"` + BalancerName + `"}`

	// ConsistentHashKey is the key for the grpc request's context.Context which points to
	// the key to hash for the request. The value it points to must be []byte
	ConsistentHashKey = CtxKeyType("taskID")

	// MaxSubConnFailTimes is max failed times to consider a SubConn as Bad SubConn and regenerate hashRing picker
	MaxSubConnFailTimes = 3
)

var (
	ErrBalancerHashring = errors.New("balancer pick node from hashRing error")
)

// NewConsistentHashRingBuilder creates a new balancer.Builder that
// will create a consistent hashring balancer with the given config.
// Before making a connection, register it with grpc with:
// `balancer.Register(consistent.NewConsistentHashringBuilder())`
func NewConsistentHashRingBuilder(gcInterval time.Duration, healthCheck bool, reloader Reloader) balancer.Builder {
	return &baseBuilder{
		BalancerName,
		Config{HealthCheck: healthCheck, GCInterval: gcInterval},
		reloader,
	}

}

type baseBuilder struct {
	name     string
	config   Config
	reloader Reloader
}

func (bb *baseBuilder) Build(cc balancer.ClientConn, opt balancer.BuildOptions) balancer.Balancer {
	bal := &consistentBalancer{
		SugaredLogger: logger.GrpcLogger.With("component", "balancer", "name", bb.name),
		cc:            cc,
		connPool:      NewConnPool(bb.config.GCInterval),
		config:        bb.config,
		reloader:      bb.reloader,
	}
	// Initialize picker to a picker that always returns
	// ErrNoSubConnAvailable, because when state of a SubConn changes, we
	// may call UpdateState with this picker.
	bal.picker = NewErrPicker(balancer.ErrNoSubConnAvailable)
	return bal
}

func (bb *baseBuilder) Name() string {
	return bb.name
}

// Config contains the config info about the base balancer builder.
type Config struct {
	// HealthCheck indicates whether health checking should be enabled for this specific balancer.
	HealthCheck bool

	// GCInterval indicates the max alive time for each ready SubConn
	GCInterval time.Duration
}

type consistentBalancer struct {
	*zap.SugaredLogger
	cc       balancer.ClientConn
	reloader Reloader

	state connectivity.State

	connPool ConnPool
	picker   balancer.Picker
	config   Config

	resolverErr error // the last error reported by the resolver; cleared on successful resolution
	connErr     error // the last connection error; cleared upon leaving TransientFailure
}

func (b *consistentBalancer) ResolverError(err error) {
	b.resolverErr = err
	if len(b.connPool.ValidKeys()) == 0 {
		b.state = connectivity.TransientFailure
	}

	if b.state != connectivity.TransientFailure {
		// The picker will not change since the balancer does not currently
		// report an error.
		return
	}
	b.regeneratePicker()
	b.cc.UpdateState(balancer.State{
		ConnectivityState: b.state,
		Picker:            b.picker,
	})
}

func (b *consistentBalancer) UpdateClientConnState(s balancer.ClientConnState) error {
	b.Infof("consistent-balancer: got new ClientConn state: %v", s)
	// Successful resolution; clear resolver error and ensure we return nil.
	b.resolverErr = nil
	// addrsSet is the set converted from addrs, it's used for quick lookup of an address.
	newAddrs := map[string]struct{}{}
	var changed bool
	for _, a := range s.ResolverState.Addresses {
		newAddrs[a.Addr] = struct{}{}
		// if addr not exist, add it
		if _, ok := b.connPool.Get(a.Addr); !ok {
			// a is a new address (not existing in b.connPool).
			changed = true
			sc, err := NewSubConnWrapper(b.cc, a.Addr, b.config.HealthCheck)
			if err != nil {
				b.Warnf("consistent-balancer: failed to create new SubConn: %v", err)
				continue
			}

			b.connPool.Set(a.Addr, sc)
		}
	}
	for _, a := range b.connPool.Keys() {
		old, _ := b.connPool.Get(a)
		// a was removed by resolver.
		if _, ok := newAddrs[a]; !ok {
			changed = true
			old.Disconnect()
			b.connPool.Remove(old.Address())
		}
	}
	// If resolver state contains no addresses, return an error so ClientConn
	// will trigger re-resolve. Also records this as an resolver error, so when
	// the overall state turns transient failure, the error message will have
	// the zero address information.
	if len(s.ResolverState.Addresses) == 0 {
		b.ResolverError(errors.New("produced zero addresses"))
		return balancer.ErrBadResolverState
	}

	if changed {
		b.regeneratePicker()
	}

	// should call cc.UpdateState even resolve state is the same as before, to trigger failed picker pick again
	b.cc.UpdateState(balancer.State{ConnectivityState: b.state, Picker: b.picker})
	return nil
}

// mergeErrors builds an error from the last connection error and the last
// resolver error.  Must only be called if b.state is TransientFailure.
func (b *consistentBalancer) mergeErrors() error {
	// connErr must always be non-nil unless there are no SubConns, in which
	// case resolverErr must be non-nil.
	if b.connErr == nil {
		return fmt.Errorf("last resolver error: %v", b.resolverErr)
	}
	if b.resolverErr == nil {
		return fmt.Errorf("last connection error: %v", b.connErr)
	}
	return fmt.Errorf("last connection error: %v; last resolver error: %v", b.connErr, b.resolverErr)
}

// regeneratePicker takes a snapshot of the balancer, and generates a picker
// from it. The picker is
//  - errPicker if the balancer is in TransientFailure,
//  - built by the pickerBuilder with all READY SubConns otherwise.
func (b *consistentBalancer) regeneratePicker() {
	if len(b.connPool.ValidKeys()) == 0 {
		b.picker = NewErrPicker(b.mergeErrors())
		// dynConfig.Reload will trigger dynConfig refresh addresses and resolver will resolve again
		if err := b.reloader.Reload(); err != nil {
			b.Errorf("balancer: trigger dynConfig reload error :%v", err)
		}
		return
	}
	b.picker = BuildPicker(b.connPool)
}

// UpdateSubConnState is the core function which control the balance state change.
// The SubConn will retry with backoff config until succeed(it will never stop retry unless the resolver told balancer to remove this node).
// But if SubConn failed to connect exceed MaxSubConnFailTimes, it will be marked as Bad node
// and regenerate a picker without Bad node
func (b *consistentBalancer) UpdateSubConnState(sc balancer.SubConn, state balancer.SubConnState) {
	s := state.ConnectivityState
	scw, ok := b.connPool.GetBySubConn(sc)
	if !ok {
		b.Debugf("consistent-balancer: got state changes for an unknown SubConn:%v", sc)
		return
	}
	b.Debugf("consistent-balancer: handle SubConn state change: %v, %v", scw.Address(), s)
	if state.ConnectionError != nil {
		b.Debugf("consistent-balancer: SubConn %v connection error:%v", scw.Address(), state.ConnectionError)
	}
	lastState := scw.State()
	isBadNode := scw.GetFailedTime() >= MaxSubConnFailTimes
	if state.ConnectivityState != connectivity.Shutdown {
		scw.UpdateState(state.ConnectivityState)
	}

	var needUpdate bool

	// b.state will be always ready after first subConn become ready
	switch state.ConnectivityState {
	case connectivity.Idle:
		// SubConn's state is transform to Idle from TransientFailure by ClientConn's backoff strategy
		if lastState == connectivity.TransientFailure { // backoff retry
			scw.Connect()
			return
		}
	case connectivity.Ready:
		b.state = connectivity.Ready
		if isBadNode {
			b.regeneratePicker()
		}
		needUpdate = true // a SubConn become ready, need update picker to tell grpc to re-pick
	case connectivity.TransientFailure:
		b.Debugf("subConn %v enter transientFailuer due to %v", scw.Address(), state.ConnectionError)
		b.connErr = state.ConnectionError
		if scw.GetFailedTime() == MaxSubConnFailTimes {
			b.regeneratePicker()
			needUpdate = true // the SubConn become a Bad node, update the picker
		}
	case connectivity.Shutdown:
		b.Infof("SubConn shutdown:%v", scw.Address())
		b.connPool.Remove(scw.Address())
		scw, err := NewSubConnWrapper(b.cc, scw.Address(), b.config.HealthCheck)
		if err != nil {
			b.Errorf("create SubConn error: %v %v", scw.Address(), err)
			return
		}
		b.connPool.Set(scw.Address(), scw)
	}

	// Regenerate picker
	if needUpdate || b.state == connectivity.TransientFailure {
		b.cc.UpdateState(balancer.State{ConnectivityState: b.state, Picker: b.picker})
	}
}

// Close is a nop because balancer doesn't have internal state to clean up,
// and it doesn't need to call RemoveSubConn for the SubConns.
func (b *consistentBalancer) Close() {
}

// ExitIdle is a nop because the balancer attempts to stay connected to
// all SubConns at all times.
func (b *consistentBalancer) ExitIdle() {
}

// NewErrPicker returns a Picker that always returns err on Pick().
func NewErrPicker(err error) balancer.Picker {
	return &errPicker{err: err}
}

type errPicker struct {
	err error // Pick() always returns this err.
}

func (p *errPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	return balancer.PickResult{}, p.err
}
