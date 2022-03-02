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
	"fmt"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/resolver"
)

var balancerLogger = grpclog.Component("balancer")

const (
	D7yBalancerPolicy = "d7y_hash_policy"
)

var (
	_ balancer.Builder  = (*d7yBalancerBuilder)(nil)
	_ balancer.Balancer = (*d7yBalancer)(nil)
)

func newD7yBalancerBuilder() balancer.Builder {
	return &d7yBalancerBuilder{
		config: Config{HealthCheck: false},
	}
}

func init() {
	balancer.Register(newD7yBalancerBuilder())
}

// d7yBalancerBuilder is a struct with functions Build and Name, implemented from balancer.Builder
type d7yBalancerBuilder struct {
	config Config
}

func (dbb *d7yBalancerBuilder) Build(cc balancer.ClientConn, _ balancer.BuildOptions) balancer.Balancer {
	b := &d7yBalancer{
		cc: cc,

		subConns:    resolver.NewAddressMap(),
		scStates:    make(map[balancer.SubConn]connectivity.State),
		csEvltr:     &ConnectivityStateEvaluator{},
		pickHistory: make(map[string]string),
		config:      dbb.config,
	}
	b.picker = base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	return b
}

// Name returns the scheme of the d7yBalancer registering in grpc.
func (dbb *d7yBalancerBuilder) Name() string {
	return D7yBalancerPolicy
}

// d7yBalancer is modified from baseBalancer, you can refer to https://github.com/grpc/grpc-go/blob/master/balancer/base/balancer.go
type d7yBalancer struct {
	// cc points to the balancer.ClientConn who creates the d7yBalancer.
	cc balancer.ClientConn

	csEvltr *ConnectivityStateEvaluator
	state   connectivity.State

	subConns    *resolver.AddressMap
	scStates    map[balancer.SubConn]connectivity.State
	pickHistory map[string]string

	// picker is a balancer.Picker created by the balancer but used by the ClientConn.
	picker balancer.Picker
	config Config

	resolverErr error // last error reported by the resolver; cleared on successful resolution.
	connErr     error // the last connection error; cleared upon leaving TransientFailure
}

// Config contains the config info about the base balancer builder.
type Config struct {
	// HealthCheck indicates whether health checking should be enabled for this specific balancer.
	HealthCheck bool
}

func (b *d7yBalancer) ResolverError(err error) {
	b.resolverErr = err
	if b.subConns.Len() == 0 {
		b.state = connectivity.TransientFailure
	}

	if b.state != connectivity.TransientFailure {
		// The picker will not change since the balancer does not currently
		// report an error.
		return
	}
	// regenerate picker
	b.regeneratePicker()
	b.cc.UpdateState(balancer.State{
		ConnectivityState: b.state,
		Picker:            b.picker,
	})
}

func (b *d7yBalancer) UpdateClientConnState(s balancer.ClientConnState) error {
	if balancerLogger.V(2) {
		balancerLogger.Info("d7yBalancer: got new ClientConn state: ", s)
	}
	// Successful resolution; clear resolver error and ensure we return nil.
	b.resolverErr = nil
	addrsSet := resolver.NewAddressMap()
	for _, addr := range s.ResolverState.Addresses {
		addrsSet.Set(addr, nil)
		if _, ok := b.subConns.Get(addr); !ok {
			// a is a new address (not existing in b.subConns)
			newSC, err := b.cc.NewSubConn([]resolver.Address{addr}, balancer.NewSubConnOptions{HealthCheckEnabled: b.config.HealthCheck})
			if err != nil {
				balancerLogger.Warningf("d7yBalancer: failed to create new SubConn: %v", err)
				continue
			}
			b.subConns.Set(addr, newSC)
			b.scStates[newSC] = connectivity.Idle
			b.state = b.csEvltr.RecordTransition(connectivity.Shutdown, connectivity.Idle)
		}
	}
	for _, addr := range b.subConns.Keys() {
		sci, _ := b.subConns.Get(addr)
		sc := sci.(balancer.SubConn)
		// addr was removed by resolver.
		if _, ok := addrsSet.Get(addr); !ok {
			b.cc.RemoveSubConn(sc)
			b.subConns.Delete(addr)
			// Keep the state of this sc in b.scStates until sc's state becomes Shutdown.
			// The entry will be deleted in UpdateSubConnState.
		}
	}

	// If resolver state contains no addresses, return an error so ClientConn
	// will trigger re-resolve. Also records this as a resolver error, so when
	// the overall state turns transient failure, the error message will have
	// the zero address information.
	if len(s.ResolverState.Addresses) == 0 {
		b.ResolverError(fmt.Errorf("produced zero addresses"))
		return balancer.ErrBadResolverState
	}

	// As we want to do the connection management ourselves, we don't set up connection here.
	// Connection has not been ready yet. The next two lines is a trick aims to make grpc continue executing.
	// if the picker is nil in ClientConn, it will block at next pick in pickerWrapper,
	// So we have to generate the picker first.
	b.regeneratePicker()
	// we call ClientConn.UpdateState() directly.
	b.cc.UpdateState(balancer.State{ConnectivityState: b.state, Picker: b.picker})

	return nil
}

// regeneratePicker generates a new picker to replace the old one with new data.
func (b *d7yBalancer) regeneratePicker() {
	//if b.state == connectivity.TransientFailure {
	//	b.picker = base.NewErrPicker(b.mergeErrors())
	//	return
	//}
	availableSubConns := make(map[resolver.Address]balancer.SubConn)
	scStates := make(map[string]connectivity.State)
	for _, addr := range b.subConns.Keys() {
		sci, _ := b.subConns.Get(addr)
		sc := sci.(balancer.SubConn)
		if st, ok := b.scStates[sc]; ok && st != connectivity.Shutdown {
			availableSubConns[addr] = sc
			scStates[addr.Addr] = st
		}
	}

	if len(availableSubConns) == 0 {
		b.picker = base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	} else {
		b.picker = newD7yPicker(d7yPickerBuildInfo{
			subConns:    availableSubConns,
			scStates:    scStates,
			pickHistory: b.pickHistory,
		})
	}
}

// mergeErrors is copied from baseBalancer.
func (b *d7yBalancer) mergeErrors() error {
	if b.connErr == nil {
		return fmt.Errorf("last resolver error: %v", b.resolverErr)
	}
	if b.resolverErr == nil {
		return fmt.Errorf("last connection error: %v", b.connErr)
	}
	return fmt.Errorf("last connection error: %v; last resolver error: %v", b.connErr, b.resolverErr)
}

// UpdateSubConnState is implemented by balancer.Balancer, modified from baseBalancer
func (b *d7yBalancer) UpdateSubConnState(sc balancer.SubConn, state balancer.SubConnState) {
	s := state.ConnectivityState
	if balancerLogger.V(2) {
		balancerLogger.Infof("d7yBalancer: handle SubConn state change: %p, %v", sc, s)
	}
	oldS, ok := b.scStates[sc]
	if !ok {
		if balancerLogger.V(2) {
			balancerLogger.Infof("d7yBalancer: got state changes for an unknown SubConn: %p, %v", sc, s)
		}
		return
	}
	if oldS == connectivity.TransientFailure && (s == connectivity.Connecting || s == connectivity.Idle) {
		// Once a subConn enters TRANSIENT_FAILURE, ignore subsequent
		// CONNECTING or Idle transitions to prevent the aggregated state from being
		// always CONNECTING when many backends exist but are all down.
		// if s == connectivity.Idle {
		//	sc.Connect()
		// }
		return
	}
	b.scStates[sc] = s
	switch s {
	case connectivity.Shutdown:
		// When an address was removed by resolver, b called RemoveSubConn but
		// kept the sc's state in scStates. Remove state for this sc here.
		delete(b.scStates, sc)
	case connectivity.TransientFailure:
		// Save error to be reported via picker.
		b.connErr = state.ConnectionError
	}

	// do not update cc if s is connecting
	if s == connectivity.Connecting {
		return
	}
	b.state = b.csEvltr.RecordTransition(oldS, s)

	// Regenerate picker when one of the following happens:
	//  - this sc entered or left shutdown/transientFailure/idle status
	//  - the aggregated state of balancer is TransientFailure
	//    (may need to update error message)
	if (s == connectivity.Shutdown) != (oldS == connectivity.Shutdown) || (s == connectivity.TransientFailure) != (oldS == connectivity.
		TransientFailure) || b.state == connectivity.Idle {
		b.regeneratePicker()
	}
	b.cc.UpdateState(balancer.State{ConnectivityState: b.state, Picker: b.picker})
}

func (b *d7yBalancer) Close() {}

func (b *d7yBalancer) ExitIdle() {
}

// ConnectivityStateEvaluator takes the connectivity states of multiple SubConns
// and returns one aggregated connectivity state.
//
// It's not thread safe.
type ConnectivityStateEvaluator struct {
	numReady            uint64 // Number of addrConns in ready state.
	numConnecting       uint64 // Number of addrConns in connecting state.
	numTransientFailure uint64 // Number of addrConns in transient failure state.
	numIdle             uint64 // Number of addrConns in idle state.
}

// RecordTransition records state change happening in subConn and based on that
// it evaluates what aggregated state should be.
//
//  - If at least one SubConn in Ready, the aggregated state is Ready;
//  - Else if at least one SubConn in Connecting, the aggregated state is Connecting;
//  - Else if at least one SubConn is Idle, the aggregated state is Idle;
//  - Else if at least one SubConn is TransientFailure, the aggregated state is Transient Failure;
//  - Else there are no subconns and the aggregated state is Transient Failure
//
// Shutdown is not considered.
func (cse *ConnectivityStateEvaluator) RecordTransition(oldState, newState connectivity.State) connectivity.State {
	// Update counters.
	for idx, state := range []connectivity.State{oldState, newState} {
		updateVal := 2*uint64(idx) - 1 // -1 for oldState and +1 for new.
		switch state {
		case connectivity.Ready:
			cse.numReady += updateVal
		case connectivity.Connecting:
			cse.numConnecting += updateVal
		case connectivity.TransientFailure:
			cse.numTransientFailure += updateVal
		case connectivity.Idle:
			cse.numIdle += updateVal
		}
	}

	// Evaluate.
	if cse.numReady > 0 {
		return connectivity.Ready
	}
	if cse.numConnecting > 0 {
		return connectivity.Connecting
	}
	if cse.numIdle > 0 {
		return connectivity.Idle
	}
	if cse.numTransientFailure > 0 {
		return connectivity.TransientFailure
	}
	return connectivity.TransientFailure
}
