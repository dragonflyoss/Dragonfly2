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
	"fmt"
	"os"
	"path/filepath"
	"time"

	"google.golang.org/grpc/resolver"

	managerv1 "d7y.io/api/pkg/apis/manager/v1"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	internaldynconfig "d7y.io/dragonfly/v2/internal/dynconfig"
	"d7y.io/dragonfly/v2/manager/searcher"
	"d7y.io/dragonfly/v2/pkg/net/ip"
	"d7y.io/dragonfly/v2/pkg/reachable"
	managerclient "d7y.io/dragonfly/v2/pkg/rpc/manager/client"
	"d7y.io/dragonfly/v2/version"
)

// Daemon cache file name.
var cacheFileName = "daemon"

type dynconfigManager struct {
	internaldynconfig.Dynconfig
	observers map[Observer]struct{}
	done      chan bool
	cachePath string
}

// newDynconfigManager returns a new manager dynconfig instence.
func newDynconfigManager(cfg *DaemonOption, rawManagerClient managerclient.Client, cacheDir string, expire time.Duration) (Dynconfig, error) {
	cachePath := filepath.Join(cacheDir, cacheFileName)
	d, err := internaldynconfig.New(
		newManagerClient(rawManagerClient, cfg),
		cachePath,
		expire,
	)
	if err != nil {
		return nil, err
	}

	return &dynconfigManager{
		observers: map[Observer]struct{}{},
		done:      make(chan bool),
		cachePath: cachePath,
		Dynconfig: d,
	}, nil
}

// Get the dynamic schedulers config from manager.
func (d *dynconfigManager) GetResolveSchedulerAddrs() ([]resolver.Address, error) {
	schedulers, err := d.GetSchedulers()
	if err != nil {
		return nil, err
	}

	var (
		addrs              = map[string]bool{}
		resolveAddrs       []resolver.Address
		schedulerClusterID uint64
	)
	for _, scheduler := range schedulers {
		// Check whether scheduler is in the same cluster.
		if schedulerClusterID != 0 && schedulerClusterID != scheduler.SchedulerClusterId {
			continue
		}

		ip, ok := ip.FormatIP(scheduler.GetIp())
		if !ok {
			continue
		}

		addr := fmt.Sprintf("%s:%d", ip, scheduler.GetPort())
		r := reachable.New(&reachable.Config{Address: addr})
		if err := r.Check(); err != nil {
			logger.Warnf("scheduler address %s is unreachable", addr)
			continue
		}

		if addrs[addr] {
			continue
		}

		schedulerClusterID = scheduler.SchedulerClusterId
		resolveAddrs = append(resolveAddrs, resolver.Address{
			ServerName: scheduler.GetIp(),
			Addr:       addr,
		})
		addrs[addr] = true
	}

	if len(resolveAddrs) == 0 {
		return nil, errors.New("can not found available scheduler addresses")
	}

	return resolveAddrs, nil
}

// Get the dynamic schedulers resolve addrs.
func (d *dynconfigManager) GetSchedulers() ([]*managerv1.Scheduler, error) {
	data, err := d.Get()
	if err != nil {
		return nil, err
	}

	return data.Schedulers, nil
}

// Get the dynamic object storage config from manager.
func (d *dynconfigManager) GetObjectStorage() (*managerv1.ObjectStorage, error) {
	data, err := d.Get()
	if err != nil {
		return nil, err
	}

	return data.ObjectStorage, nil
}

// Get the dynamic config from manager.
func (d *dynconfigManager) Get() (*DynconfigData, error) {
	var data DynconfigData
	if err := d.Unmarshal(&data); err != nil {
		return nil, err
	}

	return &data, nil
}

// Refresh refreshes dynconfig in cache.
func (d *dynconfigManager) Refresh() error {
	if err := d.Dynconfig.Refresh(); err != nil {
		return err
	}

	if err := d.Notify(); err != nil {
		return err
	}

	return nil
}

// Register allows an instance to register itself to listen/observe events.
func (d *dynconfigManager) Register(l Observer) {
	d.observers[l] = struct{}{}
}

// Deregister allows an instance to remove itself from the collection of observers/listeners.
func (d *dynconfigManager) Deregister(l Observer) {
	delete(d.observers, l)
}

// Notify publishes new events to listeners.
func (d *dynconfigManager) Notify() error {
	data, err := d.Get()
	if err != nil {
		return err
	}

	for o := range d.observers {
		o.OnNotify(data)
	}

	return nil
}

// watch the dynconfig events.
func (d *dynconfigManager) Serve() error {
	if err := d.Notify(); err != nil {
		return err
	}

	go d.watch()

	return nil
}

// watch the dynconfig events.
func (d *dynconfigManager) watch() {
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
func (d *dynconfigManager) Stop() error {
	close(d.done)
	if err := os.Remove(d.cachePath); err != nil {
		return err
	}

	return nil
}

type managerClient struct {
	managerclient.Client
	config *DaemonOption
}

// New the manager client used by dynconfig.
func newManagerClient(client managerclient.Client, cfg *DaemonOption) internaldynconfig.ManagerClient {
	return &managerClient{
		Client: client,
		config: cfg,
	}
}

func (mc *managerClient) Get() (any, error) {
	listSchedulersResp, err := mc.ListSchedulers(context.Background(), &managerv1.ListSchedulersRequest{
		SourceType: managerv1.SourceType_PEER_SOURCE,
		HostName:   mc.config.Host.Hostname,
		Ip:         mc.config.Host.AdvertiseIP,
		Version:    version.GitVersion,
		Commit:     version.GitCommit,
		HostInfo: map[string]string{
			searcher.ConditionSecurityDomain: mc.config.Host.SecurityDomain,
			searcher.ConditionIDC:            mc.config.Host.IDC,
			searcher.ConditionNetTopology:    mc.config.Host.NetTopology,
			searcher.ConditionLocation:       mc.config.Host.Location,
		},
	})
	if err != nil {
		return nil, err
	}

	if mc.config.ObjectStorage.Enable {
		getObjectStorageResp, err := mc.GetObjectStorage(context.Background(), &managerv1.GetObjectStorageRequest{
			SourceType: managerv1.SourceType_PEER_SOURCE,
			HostName:   mc.config.Host.Hostname,
			Ip:         mc.config.Host.AdvertiseIP,
		})
		if err != nil {
			return nil, err
		}

		return DynconfigData{
			Schedulers:    listSchedulersResp.Schedulers,
			ObjectStorage: getObjectStorageResp,
		}, nil
	}

	return DynconfigData{
		Schedulers: listSchedulersResp.Schedulers,
	}, nil
}
