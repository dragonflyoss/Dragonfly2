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
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/dfpath"
	dc "d7y.io/dragonfly/v2/internal/dynconfig"
	"d7y.io/dragonfly/v2/internal/rpc/manager"
	"d7y.io/dragonfly/v2/internal/rpc/manager/client"
	"d7y.io/dragonfly/v2/pkg/util/net/iputils"
)

var (
	SchedulerDynconfigPath      = filepath.Join(dfpath.WorkHome, "dynconfig/scheduler.json")
	SchedulerDynconfigCachePath = filepath.Join(dfpath.WorkHome, "dynconfig/scheduler")
)

var (
	watchInterval = 1 * time.Second
)

type DynconfigInterface interface {
	// Get the dynamic config from manager.
	Get() (*manager.SchedulerConfig, error)

	// Register allows an instance to register itself to listen/observe events.
	Register(Observer)

	// Deregister allows an instance to remove itself from the collection of observers/listeners.
	Deregister(Observer)

	// Notify publishes new events to listeners.
	Notify() error

	// Serve the dynconfig listening service.
	Serve() error

	// Stop the dynconfig listening service.
	Stop()
}

type Observer interface {
	// OnNotify allows an event to be "published" to interface implementations.
	OnNotify(*manager.SchedulerConfig)
}

type dynconfig struct {
	*dc.Dynconfig
	observers  map[Observer]struct{}
	done       chan bool
	cdnDirPath string
}

// TODO(Gaius) Rely on manager to delete cdnDirPath
func NewDynconfig(sourceType dc.SourceType, cdnDirPath string, options ...dc.Option) (DynconfigInterface, error) {
	d := &dynconfig{
		observers:  map[Observer]struct{}{},
		done:       make(chan bool),
		cdnDirPath: cdnDirPath,
	}

	client, err := dc.New(sourceType, options...)
	if err != nil {
		return nil, err
	}
	d.Dynconfig = client
	return d, nil
}

func (d *dynconfig) Get() (*manager.SchedulerConfig, error) {
	var config manager.SchedulerConfig
	if d.cdnDirPath != "" {
		cdn, err := d.getCDNFromDirPath()
		if err != nil {
			return nil, err
		}
		config.CdnHosts = cdn
	} else {
		if err := d.Unmarshal(&config); err != nil {
			return nil, err
		}
	}

	return &config, nil
}

func (d *dynconfig) getCDNFromDirPath() ([]*manager.ServerInfo, error) {
	files, err := ioutil.ReadDir(d.cdnDirPath)
	if err != nil {
		return nil, err
	}

	var data []*manager.ServerInfo
	for _, file := range files {
		// skip directory
		if file.IsDir() {
			continue
		}

		p := filepath.Join(d.cdnDirPath, file.Name())
		if file.Mode()&os.ModeSymlink != 0 {
			stat, err := os.Stat(p)
			if err != nil {
				logger.Errorf("stat %s error: %s", file.Name(), err)
				continue
			}
			// skip symbol link directory
			if stat.IsDir() {
				continue
			}
		}
		b, err := ioutil.ReadFile(p)
		if err != nil {
			return nil, err
		}

		var s *manager.ServerInfo
		if err := json.Unmarshal(b, &s); err != nil {
			return nil, err
		}

		data = append(data, s)
	}

	return data, nil
}

type managerClient struct {
	client.ManagerClient
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
			d.Notify()
		case <-d.done:
			return
		}
	}
}

func (d *dynconfig) Stop() {
	close(d.done)
}

func NewManagerClient(client client.ManagerClient) dc.ManagerClient {
	return &managerClient{client}
}

func (mc *managerClient) Get() (interface{}, error) {
	scConfig, err := mc.GetSchedulerClusterConfig(context.Background(), &manager.GetClusterConfigRequest{
		HostName: iputils.HostName,
		Type:     manager.ResourceType_Scheduler,
	})
	if err != nil {
		return nil, err
	}

	return scConfig, nil
}
