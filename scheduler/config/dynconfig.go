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

//go:generate mockgen -destination mocks/dynconfig_mock.go -source dynconfig.go -package mocks

package config

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/status"

	managerv2 "d7y.io/api/v2/pkg/apis/manager/v2"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	dc "d7y.io/dragonfly/v2/internal/dynconfig"
	"d7y.io/dragonfly/v2/manager/types"
	"d7y.io/dragonfly/v2/pkg/net/ip"
	healthclient "d7y.io/dragonfly/v2/pkg/rpc/health/client"
	managerclient "d7y.io/dragonfly/v2/pkg/rpc/manager/client"
	"d7y.io/dragonfly/v2/pkg/slices"
)

var (
	// Cache filename.
	cacheFileName = "scheduler"

	// Notify observer interval.
	watchInterval = 10 * time.Second
)

type DynconfigData struct {
	Scheduler    *managerv2.Scheduler
	Applications []*managerv2.Application
}

type DynconfigInterface interface {
	// GetResolveSeedPeerAddrs returns the dynamic schedulers resolve addrs.
	GetResolveSeedPeerAddrs() ([]resolver.Address, error)

	// GetScheduler returns the scheduler config from manager.
	GetScheduler() (*managerv2.Scheduler, error)

	// GetApplications returns the applications config from manager.
	GetApplications() ([]*managerv2.Application, error)

	// GetSeedPeers returns the dynamic seed peers config from manager.
	GetSeedPeers() ([]*managerv2.SeedPeer, error)

	// GetSchedulerCluster returns the the scheduler cluster config from manager.
	GetSchedulerCluster() (*managerv2.SchedulerCluster, error)

	// GetSchedulerClusterConfig returns the scheduler cluster config.
	GetSchedulerClusterConfig() (types.SchedulerClusterConfig, error)

	// GetSchedulerClusterClientConfig returns the client config.
	GetSchedulerClusterClientConfig() (types.SchedulerClusterClientConfig, error)

	// Get returns the dynamic config from manager.
	Get() (*DynconfigData, error)

	// Refresh refreshes dynconfig in cache.
	Refresh() error

	// Register allows an instance to register itself to listen/observe events.
	Register(Observer)

	// Deregister allows an instance to remove itself from the collection of observers/listeners.
	Deregister(Observer)

	// Notify publishes new events to listeners.
	Notify() error

	// Serve the dynconfig listening service.
	Serve() error

	// Stop the dynconfig listening service.
	Stop() error
}

type Observer interface {
	// OnNotify allows an event to be published to interface implementations.
	OnNotify(*DynconfigData)
}

type dynconfig struct {
	dc.Dynconfig[DynconfigData]
	observers            map[Observer]struct{}
	done                 chan struct{}
	cachePath            string
	transportCredentials credentials.TransportCredentials
	mu                   *sync.Mutex
}

// DynconfigOption is a functional option for configuring the dynconfig.
type DynconfigOption func(d *dynconfig) error

// WithTransportCredentials returns a DialOption which configures a connection
// level security credentials (e.g., TLS/SSL).
func WithTransportCredentials(creds credentials.TransportCredentials) DynconfigOption {
	return func(d *dynconfig) error {
		d.transportCredentials = creds
		return nil
	}
}

// NewDynconfig returns a new dynconfig instence.
func NewDynconfig(rawManagerClient managerclient.V2, cacheDir string, cfg *Config, options ...DynconfigOption) (DynconfigInterface, error) {
	cachePath := filepath.Join(cacheDir, cacheFileName)
	d := &dynconfig{
		observers: map[Observer]struct{}{},
		done:      make(chan struct{}),
		cachePath: cachePath,
		mu:        &sync.Mutex{},
	}

	for _, opt := range options {
		if err := opt(d); err != nil {
			return nil, err
		}
	}

	if rawManagerClient != nil {
		client, err := dc.New[DynconfigData](
			newManagerClient(rawManagerClient, cfg),
			cachePath,
			cfg.DynConfig.RefreshInterval,
		)
		if err != nil {
			return nil, err
		}

		d.Dynconfig = client
	}

	return d, nil
}

// GetResolveSeedPeerAddrs returns the the schedulers resolve addrs.
func (d *dynconfig) GetResolveSeedPeerAddrs() ([]resolver.Address, error) {
	seedPeers, err := d.GetSeedPeers()
	if err != nil {
		return nil, err
	}

	var (
		addrs        = map[string]bool{}
		resolveAddrs []resolver.Address
	)
	for _, seedPeer := range seedPeers {
		dialOptions := []grpc.DialOption{}
		if d.transportCredentials != nil {
			dialOptions = append(dialOptions, grpc.WithTransportCredentials(d.transportCredentials))
		} else {
			dialOptions = append(dialOptions, grpc.WithTransportCredentials(insecure.NewCredentials()))
		}

		var addr string
		if ip, ok := ip.FormatIP(seedPeer.GetIp()); ok {
			// Check health with ip address.
			target := fmt.Sprintf("%s:%d", ip, seedPeer.GetPort())
			if err := healthclient.Check(context.Background(), target, dialOptions...); err != nil {
				logger.Warnf("seed peer ip address %s is unreachable: %s", addr, err.Error())

				// Check health with host address.
				target = fmt.Sprintf("%s:%d", seedPeer.GetHostname(), seedPeer.GetPort())
				if err := healthclient.Check(context.Background(), target, dialOptions...); err != nil {
					logger.Warnf("seed peer host address %s is unreachable: %s", addr, err.Error())
				} else {
					addr = target
				}
			} else {
				addr = target
			}
		}

		if addr == "" {
			logger.Warnf("seed peer %s %s %d has not reachable addresses",
				seedPeer.GetIp(), seedPeer.GetHostname(), seedPeer.GetPort())
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
		return nil, errors.New("available seed peer not found")
	}

	return resolveAddrs, nil
}

// GetScheduler returns the scheduler config from manager.
func (d *dynconfig) GetScheduler() (*managerv2.Scheduler, error) {
	data, err := d.Get()
	if err != nil {
		return nil, err
	}

	if data.Scheduler == nil {
		return nil, errors.New("invalid scheduler")
	}

	return data.Scheduler, nil
}

// GetApplications returns the applications config from manager.
func (d *dynconfig) GetApplications() ([]*managerv2.Application, error) {
	data, err := d.Get()
	if err != nil {
		return nil, err
	}

	if len(data.Applications) == 0 {
		return nil, errors.New("application not found")
	}

	return data.Applications, nil
}

// GetSeedPeers returns the the seed peers config from manager.
func (d *dynconfig) GetSeedPeers() ([]*managerv2.SeedPeer, error) {
	scheduler, err := d.GetScheduler()
	if err != nil {
		return nil, err
	}

	if len(scheduler.SeedPeers) == 0 {
		return nil, errors.New("seed peer not found ")
	}

	return scheduler.SeedPeers, nil
}

// GetSchedulerCluster returns the the scheduler cluster config from manager.
func (d *dynconfig) GetSchedulerCluster() (*managerv2.SchedulerCluster, error) {
	scheduler, err := d.GetScheduler()
	if err != nil {
		return nil, err
	}

	if scheduler.SchedulerCluster == nil {
		return nil, errors.New("invalid scheduler cluster")
	}

	return scheduler.SchedulerCluster, nil
}

// GetSchedulerClusterConfig returns the scheduler cluster config.
func (d *dynconfig) GetSchedulerClusterConfig() (types.SchedulerClusterConfig, error) {
	schedulerCluster, err := d.GetSchedulerCluster()
	if err != nil {
		return types.SchedulerClusterConfig{}, err
	}

	var config types.SchedulerClusterConfig
	if err := json.Unmarshal(schedulerCluster.Config, &config); err != nil {
		return types.SchedulerClusterConfig{}, err
	}

	return config, nil
}

// GetSchedulerClusterClientConfig returns the client config.
func (d *dynconfig) GetSchedulerClusterClientConfig() (types.SchedulerClusterClientConfig, error) {
	schedulerCluster, err := d.GetSchedulerCluster()
	if err != nil {
		return types.SchedulerClusterClientConfig{}, err
	}

	var config types.SchedulerClusterClientConfig
	if err := json.Unmarshal(schedulerCluster.ClientConfig, &config); err != nil {
		return types.SchedulerClusterClientConfig{}, err
	}

	return config, nil
}

// Refresh refreshes dynconfig in cache.
func (d *dynconfig) Refresh() error {
	// If another load is in progress, return directly.
	if !d.mu.TryLock() {
		logger.Warn("refresh is running")
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
func (d *dynconfig) Register(l Observer) {
	d.observers[l] = struct{}{}
}

// Deregister allows an instance to remove itself from the collection of observers/listeners.
func (d *dynconfig) Deregister(l Observer) {
	delete(d.observers, l)
}

// Notify publishes new events to listeners.
func (d *dynconfig) Notify() error {
	config, err := d.Get()
	if err != nil {
		return err
	}

	for o := range d.observers {
		o.OnNotify(config)
	}

	return nil
}

// Serve the dynconfig listening service.
func (d *dynconfig) Serve() error {
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
func (d *dynconfig) Stop() error {
	close(d.done)
	if err := os.Remove(d.cachePath); err != nil {
		return err
	}

	return nil
}

// Manager client for dynconfig.
type managerClient struct {
	managerClient managerclient.V2
	config        *Config
}

// New the manager client used by dynconfig.
func newManagerClient(client managerclient.V2, cfg *Config) dc.ManagerClient {
	return &managerClient{
		managerClient: client,
		config:        cfg,
	}
}

func (mc *managerClient) Get() (any, error) {
	getSchedulerResp, err := mc.managerClient.GetScheduler(context.Background(), &managerv2.GetSchedulerRequest{
		SourceType:         managerv2.SourceType_SCHEDULER_SOURCE,
		Hostname:           mc.config.Server.Host,
		Ip:                 mc.config.Server.AdvertiseIP.String(),
		SchedulerClusterId: uint64(mc.config.Manager.SchedulerClusterID),
	})
	if err != nil {
		return nil, err
	}

	listApplicationsResp, err := mc.managerClient.ListApplications(context.Background(), &managerv2.ListApplicationsRequest{
		SourceType: managerv2.SourceType_SCHEDULER_SOURCE,
		Hostname:   mc.config.Server.Host,
		Ip:         mc.config.Server.AdvertiseIP.String(),
	})
	if err != nil {
		if s, ok := status.FromError(err); ok {
			// TODO Compatible with old version manager.
			if slices.Contains([]codes.Code{codes.Unimplemented, codes.NotFound}, s.Code()) {
				return DynconfigData{
					Scheduler:    getSchedulerResp,
					Applications: nil,
				}, nil
			}
		}

		return nil, err
	}

	return DynconfigData{
		Scheduler:    getSchedulerResp,
		Applications: listApplicationsResp.Applications,
	}, nil
}

// GetSeedPeerClusterConfigBySeedPeer returns the seed peer cluster config by seed peer.
func GetSeedPeerClusterConfigBySeedPeer(seedPeer *managerv2.SeedPeer) (types.SeedPeerClusterConfig, error) {
	if seedPeer == nil {
		return types.SeedPeerClusterConfig{}, errors.New("invalid seed peer")
	}

	if seedPeer.SeedPeerCluster == nil {
		return types.SeedPeerClusterConfig{}, errors.New("invalid seed peer cluster")
	}

	var config types.SeedPeerClusterConfig
	if err := json.Unmarshal(seedPeer.SeedPeerCluster.Config, &config); err != nil {
		return types.SeedPeerClusterConfig{}, err
	}

	return config, nil
}
