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
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	dc "d7y.io/dragonfly/v2/internal/dynconfig"
	"d7y.io/dragonfly/v2/manager/types"
	"d7y.io/dragonfly/v2/pkg/rpc/manager"
	managerclient "d7y.io/dragonfly/v2/pkg/rpc/manager/client"
)

var (
	// Cache filename.
	cacheFileName = "scheduler_dynconfig"

	// Notify observer interval.
	watchInterval = 10 * time.Second
)

type DynconfigData struct {
	SeedPeers        []*SeedPeer       `yaml:"seedPeers" mapstructure:"seedPeers" json:"seed_peers"`
	SchedulerCluster *SchedulerCluster `yaml:"schedulerCluster" mapstructure:"schedulerCluster" json:"scheduler_cluster"`
}

type SeedPeer struct {
	ID              uint             `yaml:"id" mapstructure:"id" json:"id"`
	Hostname        string           `yaml:"hostname" mapstructure:"hostname" json:"host_name"`
	Type            string           `yaml:"type" mapstructure:"type" json:"type"`
	IDC             string           `yaml:"idc" mapstructure:"idc" json:"idc"`
	NetTopology     string           `yaml:"netTopology" mapstructure:"netTopology" json:"net_topology"`
	Location        string           `yaml:"location" mapstructure:"location" json:"location"`
	IP              string           `yaml:"ip" mapstructure:"ip" json:"ip"`
	Port            int32            `yaml:"port" mapstructure:"port" json:"port"`
	DownloadPort    int32            `yaml:"downloadPort" mapstructure:"downloadPort" json:"download_port"`
	SeedPeerCluster *SeedPeerCluster `yaml:"seedPeerCluster" mapstructure:"seedPeerCluster" json:"seed_peer_cluster"`
}

func (c *SeedPeer) GetSeedPeerClusterConfig() (types.SeedPeerClusterConfig, bool) {
	if c.SeedPeerCluster == nil {
		return types.SeedPeerClusterConfig{}, false
	}

	var config types.SeedPeerClusterConfig
	if err := json.Unmarshal(c.SeedPeerCluster.Config, &config); err != nil {
		return types.SeedPeerClusterConfig{}, false
	}

	return config, true
}

type SeedPeerCluster struct {
	Config []byte `yaml:"config" mapstructure:"config" json:"config"`
}

type SchedulerCluster struct {
	Config       []byte `yaml:"config" mapstructure:"config" json:"config"`
	ClientConfig []byte `yaml:"clientConfig" mapstructure:"clientConfig" json:"client_config"`
}

type DynconfigInterface interface {
	// Get the scheduler cluster config.
	GetSchedulerClusterConfig() (types.SchedulerClusterConfig, bool)

	// Get the client config.
	GetSchedulerClusterClientConfig() (types.SchedulerClusterClientConfig, bool)

	// Get the dynamic config from manager.
	Get() (*DynconfigData, error)

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
	// OnNotify allows an event to be "published" to interface implementations.
	OnNotify(*DynconfigData)
}

type dynconfig struct {
	*dc.Dynconfig
	observers map[Observer]struct{}
	done      chan bool
	cachePath string
}

func NewDynconfig(rawManagerClient managerclient.Client, cacheDir string, cfg *Config) (DynconfigInterface, error) {
	cachePath := filepath.Join(cacheDir, cacheFileName)
	d := &dynconfig{
		observers: map[Observer]struct{}{},
		done:      make(chan bool),
		cachePath: cachePath,
	}

	if rawManagerClient != nil {
		client, err := dc.New(
			dc.ManagerSourceType,
			dc.WithCachePath(cachePath),
			dc.WithExpireTime(cfg.DynConfig.RefreshInterval),
			dc.WithManagerClient(newManagerClient(rawManagerClient, cfg)),
		)
		if err != nil {
			return nil, err
		}

		d.Dynconfig = client
	}

	return d, nil
}

func (d *dynconfig) GetSchedulerClusterConfig() (types.SchedulerClusterConfig, bool) {
	data, err := d.Get()
	if err != nil {
		return types.SchedulerClusterConfig{}, false
	}

	if data.SchedulerCluster != nil {
		return types.SchedulerClusterConfig{}, false
	}

	var config types.SchedulerClusterConfig
	if err := json.Unmarshal(data.SchedulerCluster.Config, &config); err != nil {
		return types.SchedulerClusterConfig{}, false
	}

	return config, true
}

func (d *dynconfig) GetSchedulerClusterClientConfig() (types.SchedulerClusterClientConfig, bool) {
	data, err := d.Get()
	if err != nil {
		return types.SchedulerClusterClientConfig{}, false
	}

	if data.SchedulerCluster == nil {
		return types.SchedulerClusterClientConfig{}, false
	}

	var config types.SchedulerClusterClientConfig
	if err := json.Unmarshal(data.SchedulerCluster.ClientConfig, &config); err != nil {
		return types.SchedulerClusterClientConfig{}, false
	}

	return config, true
}

func (d *dynconfig) Get() (*DynconfigData, error) {
	var config DynconfigData
	if err := d.Unmarshal(&config); err != nil {
		return nil, err
	}

	return &config, nil
}

func (d *dynconfig) Register(l Observer) {
	d.observers[l] = struct{}{}
}

func (d *dynconfig) Deregister(l Observer) {
	delete(d.observers, l)
}

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

func (d *dynconfig) Serve() error {
	if err := d.Notify(); err != nil {
		return err
	}

	go d.watch()
	return nil
}

func (d *dynconfig) watch() {
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

func (d *dynconfig) Stop() error {
	close(d.done)
	if err := os.Remove(d.cachePath); err != nil {
		return err
	}

	return nil
}

// Manager client for dynconfig.
type managerClient struct {
	managerclient.Client
	config *Config
}

func newManagerClient(client managerclient.Client, cfg *Config) dc.ManagerClient {
	return &managerClient{
		Client: client,
		config: cfg,
	}
}

func (mc *managerClient) Get() (interface{}, error) {
	scheduler, err := mc.GetScheduler(&manager.GetSchedulerRequest{
		HostName:           mc.config.Server.Host,
		SourceType:         manager.SourceType_SCHEDULER_SOURCE,
		SchedulerClusterId: uint64(mc.config.Manager.SchedulerClusterID),
	})
	if err != nil {
		return nil, err
	}

	return scheduler, nil
}
