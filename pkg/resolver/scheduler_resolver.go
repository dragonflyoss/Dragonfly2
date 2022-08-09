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

	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/resolver"

	managerv1 "d7y.io/api/pkg/apis/manager/v1"

	"d7y.io/dragonfly/v2/client/config"
)

const (
	// SchedulerScheme is the scheduler schema used by resolver.
	SchedulerScheme = "sched"
)

var (
	// SchedulerVirtualTarget is scheduler virtual target.
	SchedulerVirtualTarget = SchedulerScheme + "://localhost"
)

var plogger = grpclog.Component("scheduler_resolver")

// SchedulerResolver implement resolver.Builder
type SchedulerResolver struct {
	schedulers []*managerv1.Scheduler
	cc         resolver.ClientConn
	dynconfig  config.Dynconfig
}

// SchedulerRegister register the dragonfly resovler builder to the grpc with custom schema.
func RegisterScheduler(dynconfig config.Dynconfig) {
	resolver.Register(&SchedulerResolver{dynconfig: dynconfig})
}

// Scheme returns the resolver scheme.
func (r *SchedulerResolver) Scheme() string {
	return SchedulerScheme
}

// Build creates a new resolver for the given target.
//
// gRPC dial calls Build synchronously, and fails if the returned error is
// not nil.
func (r *SchedulerResolver) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	r.cc = cc
	r.dynconfig.Register(r)
	r.ResolveNow(resolver.ResolveNowOptions{})
	return r, nil
}

// ResolveNow will be called by gRPC to try to resolve the target name again.
// gRPC will trigger resolveNow everytime SubConn connect fail. But we only need
// to refresh addresses from manager when all SubConn fail.
// So here we don't trigger resolving to reduce the pressure of manager.
func (r *SchedulerResolver) ResolveNow(resolver.ResolveNowOptions) {
	schedulers, err := r.dynconfig.GetSchedulers()
	if err != nil {
		plogger.Errorf("get resolve addresses error %v", err)
		return
	}

	if reflect.DeepEqual(r.schedulers, schedulers) {
		plogger.Infof("resolve schedulers deep equal: %v", schedulers)
		return
	}
	r.schedulers = schedulers

	addrs := make([]resolver.Address, 0, len(schedulers))
	for _, scheduler := range schedulers {
		addr := resolver.Address{
			Addr: fmt.Sprintf("%s:%d", scheduler.GetIp(), scheduler.GetPort()),
		}

		addrs = append(addrs, addr)
	}

	plogger.Infof("update resolve addrs:%v", addrs)
	if err := r.cc.UpdateState(resolver.State{
		Addresses: addrs,
	}); err != nil {
		plogger.Errorf("resolver update ClientConn error %v", err)
	}
}

// Close closes the resolver.
func (r *SchedulerResolver) Close() {
	r.dynconfig.Deregister(r)
}

// OnNotify is triggered and resolver address is updated.
func (r *SchedulerResolver) OnNotify(data *config.DynconfigData) {
	r.ResolveNow(resolver.ResolveNowOptions{})
}
