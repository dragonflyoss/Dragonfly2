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
	"context"
	"errors"
	"net"
	"reflect"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"

	managerv1 "d7y.io/api/v2/pkg/apis/manager/v1"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	healthclient "d7y.io/dragonfly/v2/pkg/rpc/health/client"
)

var (
	ErrUnimplemented = errors.New("operation is not implemented")
)

type dynconfigLocal struct {
	config               *DaemonOption
	observers            map[Observer]struct{}
	done                 chan struct{}
	transportCredentials credentials.TransportCredentials
}

// newDynconfigLocal returns a new local dynconfig instence.
func newDynconfigLocal(cfg *DaemonOption, creds credentials.TransportCredentials) (Dynconfig, error) {
	return &dynconfigLocal{
		config:               cfg,
		observers:            map[Observer]struct{}{},
		done:                 make(chan struct{}),
		transportCredentials: creds,
	}, nil
}

// Get the dynamic seed peers config.
func (d *dynconfigLocal) GetSeedPeers() ([]*managerv1.SeedPeer, error) {
	return nil, ErrUnimplemented
}

// Get the dynamic schedulers resolve addrs.
func (d *dynconfigLocal) GetResolveSchedulerAddrs() ([]resolver.Address, error) {
	var (
		addrs        = map[string]bool{}
		resolveAddrs = []resolver.Address{}
	)
	for _, schedulerAddr := range d.config.Scheduler.NetAddrs {
		dialOptions := []grpc.DialOption{}
		if d.transportCredentials != nil {
			dialOptions = append(dialOptions, grpc.WithTransportCredentials(d.transportCredentials))
		} else {
			dialOptions = append(dialOptions, grpc.WithTransportCredentials(insecure.NewCredentials()))
		}

		addr := schedulerAddr.Addr
		if err := healthclient.Check(context.Background(), addr, dialOptions...); err != nil {
			logger.Warnf("scheduler address %s is unreachable: %s", addr, err.Error())
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

	if len(resolveAddrs) == 0 {
		return nil, errors.New("can not found available scheduler addresses")
	}

	return resolveAddrs, nil
}

// Get the dynamic schedulers config from local.
func (d *dynconfigLocal) GetSchedulers() ([]*managerv1.Scheduler, error) {
	return nil, ErrUnimplemented
}

// Get the dynamic schedulers cluster id. The local dynamic configuration does not support
// get the scheduler cluster id.
func (d *dynconfigLocal) GetSchedulerClusterID() uint64 {
	return 0
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
	data := &DynconfigData{}
	for _, schedulerAddr := range d.config.Scheduler.NetAddrs {
		addr := schedulerAddr.Addr
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			continue
		}

		p, err := strconv.ParseInt(port, 10, 32)
		if err != nil {
			continue
		}

		data.Schedulers = append(data.Schedulers, &managerv1.Scheduler{
			Hostname: host,
			Port:     int32(p),
		})
	}

	for o := range d.observers {
		o.OnNotify(data)
	}

	return nil
}

// OnNotify allows an event to be published to the dynconfig.
// Used for listening changes of the local configuration.
func (d *dynconfigLocal) OnNotify(cfg *DaemonOption) {
	if reflect.DeepEqual(d.config, cfg) {
		return
	}

	d.config = cfg
}

// Serve the dynconfig listening service.
func (d *dynconfigLocal) Serve() error {
	if err := d.Notify(); err != nil {
		return err
	}

	tick := time.NewTicker(watchInterval)
	for {
		select {
		case <-tick.C:
			if err := d.Notify(); err != nil {
				logger.Error("dynconfig notify failed", err)
			}
		case <-d.done:
			return nil
		}
	}
}

// Stop the dynconfig listening service.
func (d *dynconfigLocal) Stop() error {
	close(d.done)
	return nil
}
