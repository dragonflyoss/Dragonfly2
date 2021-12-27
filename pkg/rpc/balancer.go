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
	"fmt"
	"github.com/pkg/errors"
	"log"
	"time"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/resolver"
)

const (
	D7yBalancerPolicy  = "d7y_hash_policy"
	connectionLifetime = 10 * time.Minute
)

var (
	_ balancer.Builder  = (*d7yBalancerBuilder)(nil)
	_ balancer.Balancer = (*d7yBalancer)(nil)

	ErrSubConnNotFound  = errors.New("SubConn not found")
	ErrResetSubConnFail = errors.New("reset SubConn fail")
)

func newD7yBalancerBuilder(name string, config Config) balancer.Builder {
	return &d7yBalancerBuilder{
		name:   name,
		config: config,
	}
}

func init() {
	balancer.Register(newD7yBalancerBuilder("cdn", Config{HealthCheck: false}))
}

// d7yBalancerBuilder is a struct with functions Build and Name, implemented from balancer.Builder
type d7yBalancerBuilder struct {
	name   string
	config Config
}

// Build creates a d7yBalancer, and starts its scManager.
func (dbb *d7yBalancerBuilder) Build(cc balancer.ClientConn, opts balancer.BuildOptions) balancer.Balancer {
	b := &d7yBalancer{
		cc:       cc,
		config:   dbb.config,
		subConns: resolver.NewAddressMap(),
		scStates: make(map[balancer.SubConn]connectivity.State),
		csEvltr:  &balancer.ConnectivityStateEvaluator{},
	}
	b.picker = base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	return b
}

// Name returns the scheme of the d7yBalancer registering in grpc.
func (dbb *d7yBalancerBuilder) Name() string {
	return D7yBalancerPolicy
}

type SubConnInfo struct {
	Address resolver.Address // the address used to create this SubConn
}

type subConnPickRecord struct {
	ctxs       []context.Context
	accessTime time.Time
}

// d7yBalancer is modified from baseBalancer, you can refer to https://github.com/grpc/grpc-go/blob/master/balancer/base/balancer.go
type d7yBalancer struct {
	// cc points to the balancer.ClientConn who creates the d7yBalancer.
	cc balancer.ClientConn

	csEvltr *balancer.ConnectivityStateEvaluator
	// state indicates the state of the whole ClientConn from the perspective of the balancer, it will be changed during regeneratePicker.
	state connectivity.State

	subConns *resolver.AddressMap
	scStates map[balancer.SubConn]connectivity.State
	// picker is a balancer.Picker created by the balancer but used by the ClientConn.
	picker balancer.Picker
	config Config

	// resolverErr is the last error reported by the resolver; cleared on successful resolution.
	resolverErr error
	// connErr is the last connection error; cleared upon leaving TransientFailure
	connErr error
}

// Config contains the config info about the base balancer builder.
type Config struct {
	// HealthCheck indicates whether health checking should be enabled for this specific balancer.
	HealthCheck bool
}

// ResolverError is implemented from balancer.Balancer, modified from baseBalancer, and update the state of the balancer.
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
	b.regeneratePicker()
	b.cc.UpdateState(balancer.State{
		ConnectivityState: b.state,
		Picker:            b.picker,
	})
}

// UpdateClientConnState is implemented from balancer.Balancer, modified from the baseBalancer,
// ClientConn will call it after Builder builds the balancer to pass the necessary data.
func (b *d7yBalancer) UpdateClientConnState(s balancer.ClientConnState) error {
	// clear resolver error
	b.resolverErr = nil
	addrsSet := resolver.NewAddressMap()
	for _, a := range s.ResolverState.Addresses {
		addrsSet.Set(a, nil)
		addr := a.Addr
		if sc, ok := b.subConns.Get(a); !ok {
			newSC, err := b.cc.NewSubConn([]resolver.Address{a}, balancer.NewSubConnOptions{HealthCheckEnabled: b.config.HealthCheck})
			if err != nil {
				log.Printf("d7yBalancer: failed to create new SubConn: %v", err)
				continue
			}
			b.subConns.Set(a, newSC)
			b.scStates[newSC] = connectivity.Idle
			b.csEvltr.RecordTransition(connectivity.Shutdown, connectivity.Idle)
			// newSC.Connect()
			// newSC.Connect() will lead to a function call for balancer.UpdateSubConnState(),
			// and the picker will be generated at the first time in the design of the baseBalancer.

			// PickerWrapper will block if the picker is nil, or the picker.Pick returns a bad SubConn,
			// and it will be unblocked when ClientConn.UpdateState() is called.
			// Because we do not connect here, grpc will not enter UpdateSubConn or other functions that will call ClientConn.UpdateState() with a picker.
			// and the programme will be blocked by the pickerWrapper permanently.
			// We should use another way to achieve ClientConn.UpdateState() with a picker before the first pick,
			// for example, we call ClientConn.UpdateState() directly.
		}
	}
	for a, sc := range b.subConns.Keys() {
		if _, ok := b.subConns.Get(a); !ok {
			b.cc.RemoveSubConn(sc)
			delete(b.addrInfos, a)
			delete(b.subConns, a)
		}
	}
	b.baseInfosLock.Unlock()

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
	if b.state == connectivity.TransientFailure {
		b.picker = base.NewErrPicker(b.mergeErrors())
		return
	}
	availableSCs := make(map[string]balancer.SubConn)

	for addr, sc := range b.subConns.Keys() {
		if st, ok := b.scInfos.Load(sc); ok {
			info := st.(*subConnInfo)
			// The next line may not be safe, but we have to use subConns without check,
			// for we have not set up connections with any SubConn, all of them are Idle.
			if info.state != connectivity.Shutdown {
				availableSCs[addr] = sc
			}
		}
	}
	b.baseInfosLock.RUnlock()

	if len(availableSCs) == 0 {
		b.state = connectivity.TransientFailure
		b.picker = base.NewErrPicker(b.mergeErrors())
	} else {
		b.state = connectivity.Ready
		b.picker = newD7yPicker(availableSCs, b.pickResultChan, &b.pickHistory)
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
	v, ok := b.scInfos.Load(sc)
	if !ok {
		return
	}
	info := v.(*subConnInfo)
	oldS := info.state
	if oldS == connectivity.TransientFailure && s == connectivity.Connecting {
		return
	}
	info.state = s
	switch s {
	case connectivity.Idle:
		// I do not know when it will come here.
		// sc.Connect()
	case connectivity.Shutdown:
		b.baseInfosLock.Lock()
		v, _ := b.scInfos.Load(sc)
		addr := v.(*subConnInfo).addr
		delete(b.subConns, addr)
		delete(b.addrInfos, addr)
		b.scInfos.Delete(sc)
		b.baseInfosLock.Unlock()
	case connectivity.TransientFailure:
		b.connErr = state.ConnectionError
	}

	if (s == connectivity.Shutdown) != (oldS == connectivity.Shutdown) || b.state == connectivity.TransientFailure {
		b.regeneratePicker()
	}

	b.cc.UpdateState(balancer.State{ConnectivityState: b.state, Picker: b.picker})
}

// Close is implemented by balancer.Balancer, copied from baseBalancer.
func (b *d7yBalancer) Close() {}

// resetSubConn will replace a SubConn with a new idle SubConn.
func (b *d7yBalancer) resetSubConn(sc balancer.SubConn) error {
	addr, err := b.getSubConnAddr(sc)
	if err != nil {
		return err
	}
	log.Printf("Reset connect with addr %s", addr)
	err = b.resetSubConnWithAddr(addr)
	return err
}

// getSubConnAddr returns the address string of a SubConn.
func (b *d7yBalancer) getSubConnAddr(sc balancer.SubConn) (string, error) {
	v, ok := b.scInfos.Load(sc)
	if !ok {
		return "", ErrSubConnNotFound
	}
	return v.(*subConnInfo).addr, nil
}

// resetSubConnWithAddr creates a new idle SubConn for the address string, and remove the old one.
func (b *d7yBalancer) resetSubConnWithAddr(addr string) error {
	b.baseInfosLock.Lock()
	sc, ok := b.subConns[addr]
	if !ok {
		return ErrSubConnNotFound
	}
	b.scInfos.Delete(sc)
	b.cc.RemoveSubConn(sc)
	newSC, err := b.cc.NewSubConn([]resolver.Address{b.addrInfos[addr]}, balancer.NewSubConnOptions{HealthCheckEnabled: false})
	b.baseInfosLock.Unlock()
	if err != nil {
		log.Printf("Consistent Hash Balancer: failed to create new SubConn: %v", err)
		return ErrResetSubConnFail
	}

	b.baseInfosLock.Lock()
	b.subConns[addr] = newSC
	b.scInfos.Store(newSC, &subConnInfo{
		state: connectivity.Idle,
		addr:  addr,
	})
	b.baseInfosLock.Unlock()

	b.regeneratePicker()
	b.cc.UpdateState(balancer.State{ConnectivityState: b.state, Picker: b.picker})
	return nil
}

// scManager launches two goroutines to receive PickResult and manage the subConns.
func (b *d7yBalancer) scManager() {
	// The first goroutine listens to the pickResultChan, put pickResults into subConnPickRecords map.
	go func() {
		for {
			pickResult := <-b.pickResultChan
			b.pickInfosLock.Lock()
			b.pickHistory.Store(pickResult.Key, pickResult.TargetAddr)
			if v, ok := b.subConnPickRecords.Load(pickResult.SC); ok {
				pickRecord := v.(*subConnPickRecord)
				pickRecord.ctxs = append(pickRecord.ctxs, pickResult.Ctx)
				pickRecord.accessTime = pickResult.PickTime
			} else {
				b.subConnPickRecords.Store(pickResult.SC,
					&subConnPickRecord{
						ctxs:       []context.Context{pickResult.Ctx},
						accessTime: pickResult.PickTime,
					},
				)
			}
			b.pickInfosLock.Unlock()
		}
	}()

	// The second goroutine check the subConnPickRecords map, resets the SubConn alive for more than connectionLifetime.
	go func() {
		ticker := time.NewTicker(2 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			b.pickInfosLock.Lock()
			b.pickHistory.Range(func(key, value interface{}) bool {
				b.baseInfosLock.RLock()
				addr := value.(string)
				subConn := b.subConns[addr]
				_pickRecord, _ := b.subConnPickRecords.Load(subConn)
				pickRecord := _pickRecord.(*subConnPickRecord)
				for i, ctx := range pickRecord.ctxs {
					select {
					case <-ctx.Done():
						pickRecord.ctxs = append(pickRecord.ctxs[:i], pickRecord.ctxs[i+1:]...)
					default:
						pickRecord.accessTime = time.Now()
					}
				}
				if len(pickRecord.ctxs) == 0 {
					if pickRecord.accessTime.Add(connectionLifetime).Before(time.Now()) {
						b.pickHistory.Delete(key)
						if err := b.resetSubConn(subConn); err != nil {
							log.Printf("d7yBalancer: failed to reset subConn: %v", err)
						}
						b.subConnPickRecords.Delete(subConn)
					}
				}
				b.baseInfosLock.RUnlock()
				return true
			})
			b.pickInfosLock.Unlock()
		}
	}()
}
