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
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"

	managerv1 "d7y.io/api/v2/pkg/apis/manager/v1"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	internaldynconfig "d7y.io/dragonfly/v2/internal/dynconfig"
	"d7y.io/dragonfly/v2/manager/searcher"
	"d7y.io/dragonfly/v2/pkg/net/ip"
	healthclient "d7y.io/dragonfly/v2/pkg/rpc/health/client"
	managerclient "d7y.io/dragonfly/v2/pkg/rpc/manager/client"
	"d7y.io/dragonfly/v2/version"
)

// Daemon cache file name.
var cacheFileName = "daemon"

type dynconfigManager struct {
	config *DaemonOption
	internaldynconfig.Dynconfig[DynconfigData]
	observers            map[Observer]struct{}
	done                 chan struct{}
	cachePath            string
	transportCredentials credentials.TransportCredentials
	schedulerClusterID   uint64
	mu                   sync.Mutex
}

// newDynconfigManager returns a new manager dynconfig instence.
func newDynconfigManager(cfg *DaemonOption, rawManagerClient managerclient.V1, cacheDir string, creds credentials.TransportCredentials) (Dynconfig, error) {
	cachePath := filepath.Join(cacheDir, cacheFileName)
	d, err := internaldynconfig.New[DynconfigData](
		newManagerClient(rawManagerClient, cfg),
		cachePath,
		cfg.Scheduler.Manager.RefreshInterval,
	)
	if err != nil {
		return nil, err
	}

	return &dynconfigManager{
		config:               cfg,
		observers:            map[Observer]struct{}{},
		done:                 make(chan struct{}),
		cachePath:            cachePath,
		Dynconfig:            d,
		transportCredentials: creds,
		mu:                   sync.Mutex{},
	}, nil
}

// Get the dynamic seed peers config.
func (d *dynconfigManager) GetSeedPeers() ([]*managerv1.SeedPeer, error) {
	data, err := d.Get()
	if err != nil {
		return nil, err
	}

	if len(data.SeedPeers) == 0 {
		return nil, errors.New("seed peers not found")
	}

	return data.SeedPeers, nil
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

		dialOptions := []grpc.DialOption{}
		if d.transportCredentials != nil {
			dialOptions = append(dialOptions, grpc.WithTransportCredentials(d.transportCredentials))
		} else {
			dialOptions = append(dialOptions, grpc.WithTransportCredentials(insecure.NewCredentials()))
		}

		var addr string
		if ip, ok := ip.FormatIP(scheduler.GetIp()); ok {
			// Check health with ip address.
			target := fmt.Sprintf("%s:%d", ip, scheduler.GetPort())
			if err := healthclient.Check(context.Background(), target, dialOptions...); err != nil {
				logger.Warnf("scheduler ip address %s is unreachable: %s", target, err.Error())

				// Check health with host address.
				target = fmt.Sprintf("%s:%d", scheduler.GetHostname(), scheduler.GetPort())
				if err := healthclient.Check(context.Background(), target, dialOptions...); err != nil {
					logger.Warnf("scheduler host address %s is unreachable: %s", target, err.Error())
				} else {
					addr = target
				}
			} else {
				addr = target
			}
		}

		if addr == "" {
			logger.Warnf("scheduler %s %s %d has not reachable addresses",
				scheduler.GetIp(), scheduler.GetHostname(), scheduler.GetPort())
			continue
		}

		if addrs[addr] {
			continue
		}

		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			continue
		}

		schedulerClusterID = scheduler.SchedulerClusterId
		resolveAddrs = append(resolveAddrs, resolver.Address{
			ServerName: host,
			Addr:       addr,
		})
		addrs[addr] = true
	}

	if len(resolveAddrs) == 0 {
		return nil, errors.New("can not found available scheduler addresses")
	}

	d.schedulerClusterID = schedulerClusterID
	return resolveAddrs, nil
}

// Get the dynamic schedulers resolve addrs.
func (d *dynconfigManager) GetSchedulers() ([]*managerv1.Scheduler, error) {
	data, err := d.Get()
	if err != nil {
		return nil, err
	}

	if len(data.Schedulers) == 0 {
		return nil, errors.New("schedulers not found")
	}

	return data.Schedulers, nil
}

// Get the dynamic schedulers cluster id.
func (d *dynconfigManager) GetSchedulerClusterID() uint64 {
	return d.schedulerClusterID
}

// Get the dynamic object storage config from manager.
func (d *dynconfigManager) GetObjectStorage() (*managerv1.ObjectStorage, error) {
	data, err := d.Get()
	if err != nil {
		return nil, err
	}

	if data.ObjectStorage == nil {
		return nil, errors.New("invalid object storage")
	}

	return data.ObjectStorage, nil
}

// Refresh refreshes dynconfig in cache.
func (d *dynconfigManager) Refresh() error {
	// If another load is in progress, return directly.
	if !d.mu.TryLock() {
		return nil
	}
	defer d.mu.Unlock()

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

// OnNotify allows an event to be published to the dynconfig.
// Used for listening changes of the local configuration.
func (d *dynconfigManager) OnNotify(cfg *DaemonOption) {
	if reflect.DeepEqual(d.config, cfg) {
		return
	}

	d.config = cfg
}

// Serve the dynconfig listening service.
func (d *dynconfigManager) Serve() error {
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
func (d *dynconfigManager) Stop() error {
	close(d.done)
	if err := os.Remove(d.cachePath); err != nil {
		return err
	}

	return nil
}

type managerClient struct {
	managerClient managerclient.V1
	config        *DaemonOption
}

// New the manager client used by dynconfig.
func newManagerClient(client managerclient.V1, cfg *DaemonOption) internaldynconfig.ManagerClient {
	return &managerClient{
		managerClient: client,
		config:        cfg,
	}
}

func (mc *managerClient) Get() (any, error) {
	data := DynconfigData{}

	listSchedulersResp, err := mc.managerClient.ListSchedulers(context.Background(), &managerv1.ListSchedulersRequest{
		SourceType: managerv1.SourceType_PEER_SOURCE,
		Hostname:   mc.config.Host.Hostname,
		Ip:         mc.config.Host.AdvertiseIP.String(),
		Version:    version.GitVersion,
		Commit:     version.GitCommit,
		HostInfo: map[string]string{
			searcher.ConditionIDC:      mc.config.Host.IDC,
			searcher.ConditionLocation: mc.config.Host.Location,
		},
	})
	if err != nil {
		return nil, err
	}
	data.Schedulers = listSchedulersResp.Schedulers

	if mc.config.Scheduler.Manager.SeedPeer.Enable {
		listSeedPeersResp, err := mc.managerClient.ListSeedPeers(context.Background(), &managerv1.ListSeedPeersRequest{
			SourceType: managerv1.SourceType_PEER_SOURCE,
			Hostname:   mc.config.Host.Hostname,
			Ip:         mc.config.Host.AdvertiseIP.String(),
		})
		if err != nil {
			logger.Warnf("list seed peers failed: %s", err.Error())
		} else {
			data.SeedPeers = listSeedPeersResp.SeedPeers
		}
	}

	if mc.config.ObjectStorage.Enable {
		getObjectStorageResp, err := mc.managerClient.GetObjectStorage(context.Background(), &managerv1.GetObjectStorageRequest{
			SourceType: managerv1.SourceType_PEER_SOURCE,
			Hostname:   mc.config.Host.Hostname,
			Ip:         mc.config.Host.AdvertiseIP.String(),
		})
		if err != nil {
			return nil, err
		}

		data.ObjectStorage = getObjectStorageResp
	}

	return data, nil
}
