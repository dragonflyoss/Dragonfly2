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

package config

import (
	"errors"
	"time"

	"google.golang.org/grpc/resolver"

	managerv1 "d7y.io/api/pkg/apis/manager/v1"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/reachable"
	"d7y.io/dragonfly/v2/pkg/slices"
)

var (
	ErrUnimplemented = errors.New("operation is not implemented")
)

type dynconfigLocal struct {
	config    *DaemonOption
	observers map[Observer]struct{}
	done      chan bool
}

// newDynconfigLocal returns a new local dynconfig instence.
func newDynconfigLocal(cfg *DaemonOption) (Dynconfig, error) {
	return &dynconfigLocal{
		config:    cfg,
		observers: map[Observer]struct{}{},
		done:      make(chan bool),
	}, nil
}

func (d *dynconfigLocal) GetResolveSchedulerAddrs() ([]resolver.Address, error) {
	addrs := []string{}
	for _, schedulerAddr := range d.config.Scheduler.NetAddrs {
		r := reachable.New(&reachable.Config{Address: schedulerAddr.Addr})
		if err := r.Check(); err != nil {
			logger.Warnf("scheduler address %s is unreachable", schedulerAddr.Addr)
		} else {
			addrs = append(addrs, schedulerAddr.Addr)
			continue
		}
	}

	resolveAddrs := []resolver.Address{}
	for _, addr := range slices.RemoveDuplicates(addrs) {
		resolveAddrs = append(resolveAddrs, resolver.Address{
			Addr: addr,
		})
	}

	return resolveAddrs, nil
}

func (d *dynconfigLocal) GetSchedulers() ([]*managerv1.Scheduler, error) {
	return nil, ErrUnimplemented
}

func (d *dynconfigLocal) GetObjectStorage() (*managerv1.ObjectStorage, error) {
	return nil, ErrUnimplemented
}

func (d *dynconfigLocal) Get() (*DynconfigData, error) {
	return nil, ErrUnimplemented
}

func (d *dynconfigLocal) Register(l Observer) {
	d.observers[l] = struct{}{}
}

func (d *dynconfigLocal) Deregister(l Observer) {
	delete(d.observers, l)
}

func (d *dynconfigLocal) Notify() error {
	data, err := d.Get()
	if err != nil {
		return err
	}

	for o := range d.observers {
		o.OnNotify(data)
	}

	return nil
}

func (d *dynconfigLocal) Serve() error {
	if err := d.Notify(); err != nil {
		return err
	}

	go d.watch()

	return nil
}

func (d *dynconfigLocal) watch() {
	tick := time.NewTicker(watchInterval)

	for {
		select {
		case <-tick.C:
			if err := d.Notify(); err != nil {
				logger.Error("dynconfig notify failed", err)
			}
		case <-d.done:
			return
		}
	}
}

func (d *dynconfigLocal) Stop() error {
	close(d.done)
	return nil
}
