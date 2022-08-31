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
	"net"
	"time"

	"google.golang.org/grpc/resolver"

	managerv1 "d7y.io/api/pkg/apis/manager/v1"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/reachable"
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

// Get the dynamic schedulers resolve addrs.
func (d *dynconfigLocal) GetResolveSchedulerAddrs() ([]resolver.Address, error) {
	var (
		addrs        = map[string]bool{}
		resolveAddrs = []resolver.Address{}
	)
	for _, schedulerAddr := range d.config.Scheduler.NetAddrs {
		addr := schedulerAddr.Addr
		r := reachable.New(&reachable.Config{Address: addr})
		if err := r.Check(); err != nil {
			logger.Warnf("scheduler address %s is unreachable", addr)
			continue
		}

		if addrs[addr] {
			continue
		}

		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			continue
		}

		resolveAddrs = append(resolveAddrs, resolver.Address{
			ServerName: host,
			Addr:       addr,
		})
		addrs[addr] = true
	}

	return resolveAddrs, nil
}

// Get the dynamic schedulers config from local.
func (d *dynconfigLocal) GetSchedulers() ([]*managerv1.Scheduler, error) {
	return nil, ErrUnimplemented
}

// Get the dynamic object storage config from local.
func (d *dynconfigLocal) GetObjectStorage() (*managerv1.ObjectStorage, error) {
	return nil, ErrUnimplemented
}

// Get the dynamic config from local.
func (d *dynconfigLocal) Get() (*DynconfigData, error) {
	return nil, ErrUnimplemented
}

// Refresh refreshes dynconfig in cache.
func (d *dynconfigLocal) Refresh() error {
	return nil
}

// Register allows an instance to register itself to listen/observe events.
func (d *dynconfigLocal) Register(l Observer) {
	d.observers[l] = struct{}{}
}

// Deregister allows an instance to remove itself from the collection of observers/listeners.
func (d *dynconfigLocal) Deregister(l Observer) {
	delete(d.observers, l)
}

// Notify publishes new events to listeners.
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

// Serve the dynconfig listening service.
func (d *dynconfigLocal) Serve() error {
	if err := d.Notify(); err != nil {
		return err
	}

	go d.watch()
	return nil
}

// watch the dynconfig events.
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

// Stop the dynconfig listening service.
func (d *dynconfigLocal) Stop() error {
	close(d.done)
	return nil
}
