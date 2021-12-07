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

package config

import (
	"os"
	"path/filepath"
	"time"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	internaldynconfig "d7y.io/dragonfly/v2/internal/dynconfig"
	"d7y.io/dragonfly/v2/manager/searcher"
	"d7y.io/dragonfly/v2/pkg/rpc/manager"
	managerclient "d7y.io/dragonfly/v2/pkg/rpc/manager/client"
)

var (
	// Watch dynconfig interval
	watchInterval = 10 * time.Second
)

type DynconfigData struct {
	Schedulers []*manager.Scheduler
}

type Dynconfig interface {
	// Get the dynamic config from manager.
	GetSchedulers() ([]*manager.Scheduler, error)

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
	*internaldynconfig.Dynconfig
	observers map[Observer]struct{}
	done      chan bool
}

func NewDynconfig(rawManagerClient managerclient.Client, cacheDir string, hostOption HostOption, expire time.Duration) (Dynconfig, error) {
	cachePath := filepath.Join(cacheDir, "daemon_dynconfig")
	client, err := internaldynconfig.New(
		internaldynconfig.ManagerSourceType,
		internaldynconfig.WithManagerClient(newManagerClient(rawManagerClient, hostOption)),
		internaldynconfig.WithExpireTime(expire),
		internaldynconfig.WithCachePath(cachePath),
	)
	if err != nil {
		return nil, err
	}

	return &dynconfig{
		observers: map[Observer]struct{}{},
		done:      make(chan bool),
		Dynconfig: client,
	}, nil
}

func (d *dynconfig) GetSchedulers() ([]*manager.Scheduler, error) {
	data, err := d.Get()
	if err != nil {
		return nil, err
	}

	return data.Schedulers, nil
}

func (d *dynconfig) Get() (*DynconfigData, error) {
	var data DynconfigData
	if err := d.Unmarshal(&data); err != nil {
		return nil, err
	}

	return &data, nil
}

func (d *dynconfig) Register(l Observer) {
	d.observers[l] = struct{}{}
}

func (d *dynconfig) Deregister(l Observer) {
	delete(d.observers, l)
}

func (d *dynconfig) Notify() error {
	data, err := d.Get()
	if err != nil {
		return err
	}

	for o := range d.observers {
		o.OnNotify(data)
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
	if err := os.Remove(cachePath); err != nil {
		return err
	}

	return nil
}

type managerClient struct {
	managerclient.Client
	hostOption HostOption
}

// New the manager client used by dynconfig
func newManagerClient(client managerclient.Client, hostOption HostOption) internaldynconfig.ManagerClient {
	return &managerClient{
		Client:     client,
		hostOption: hostOption,
	}
}

func (mc *managerClient) Get() (interface{}, error) {
	schedulers, err := mc.ListSchedulers(&manager.ListSchedulersRequest{
		SourceType: manager.SourceType_CLIENT_SOURCE,
		HostName:   mc.hostOption.Hostname,
		Ip:         mc.hostOption.AdvertiseIP,
		HostInfo: map[string]string{
			searcher.ConditionSecurityDomain: mc.hostOption.SecurityDomain,
			searcher.ConditionIDC:            mc.hostOption.IDC,
			searcher.ConditionNetTopology:    mc.hostOption.NetTopology,
			searcher.ConditionLocation:       mc.hostOption.Location,
		},
	})
	if err != nil {
		return nil, err
	}

	return schedulers, nil
}
