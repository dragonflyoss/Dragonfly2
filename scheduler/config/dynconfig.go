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
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/dfpath"
	dc "d7y.io/dragonfly/v2/internal/dynconfig"
	"d7y.io/dragonfly/v2/pkg/rpc/manager"
	managerclient "d7y.io/dragonfly/v2/pkg/rpc/manager/client"
	"d7y.io/dragonfly/v2/pkg/util/net/iputils"
)

var (
	DefaultDynconfigCachePath = filepath.Join(dfpath.DefaultCacheDir, "scheduler_dynconfig")
)

var (
	watchInterval = 1 * time.Second
)

type DynconfigData struct {
	CDNs []*CDN `yaml:"cdns" mapstructure:"cdns"`
}

type CDN struct {
	HostName      string `yaml:"hostname" mapstructure:"hostname" json:"host_name"`
	IP            string `yaml:"ip" mapstructure:"ip" json:"ip"`
	Port          int32  `yaml:"port" mapstructure:"port" json:"port"`
	DownloadPort  int32  `yaml:"downloadPort" mapstructure:"downloadPort" json:"download_port"`
	SecurityGroup string `yaml:"securityGroup" mapstructure:"securityGroup" json:"security_group"`
	Location      string `yaml:"location" mapstructure:"location" json:"location"`
	IDC           string `yaml:"idc" mapstructure:"idc" json:"idc"`
	NetTopology   string `yaml:"netTopology" mapstructure:"netTopology" json:"net_topology"`
	LoadLimit     int32  `yaml:"loadLimit" mapstructure:"loadLimit" json:"load_limit"`
}

type DynconfigInterface interface {
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
	Stop()
}

type Observer interface {
	// OnNotify allows an event to be "published" to interface implementations.
	OnNotify(*DynconfigData)
}

type dynconfig struct {
	*dc.Dynconfig
	observers  map[Observer]struct{}
	done       chan bool
	cdnDirPath string
	sourceType dc.SourceType
}

// TODO(Gaius) Rely on manager to delete cdnDirPath
func NewDynconfig(sourceType dc.SourceType, cdnDirPath string, options ...dc.Option) (DynconfigInterface, error) {
	d := &dynconfig{
		observers:  map[Observer]struct{}{},
		done:       make(chan bool),
		cdnDirPath: cdnDirPath,
		sourceType: sourceType,
	}

	client, err := dc.New(sourceType, options...)
	if err != nil {
		return nil, err
	}
	d.Dynconfig = client
	return d, nil
}

func (d *dynconfig) Get() (*DynconfigData, error) {
	var config DynconfigData
	if d.cdnDirPath != "" {
		cdns, err := d.getCDNFromDirPath()
		if err != nil {
			return nil, err
		}
		config.CDNs = cdns
		return &config, nil
	}

	if d.sourceType == dc.ManagerSourceType {
		if err := d.Unmarshal(&config); err != nil {
			return nil, err
		}
		return &config, nil
	}

	if err := d.Unmarshal(&struct {
		Dynconfig *DynConfig `yaml:"dynconfig" mapstructure:"dynconfig"`
	}{
		Dynconfig: &DynConfig{
			Data: &config,
		},
	}); err != nil {
		return nil, err
	}
	return &config, nil
}

func (d *dynconfig) getCDNFromDirPath() ([]*CDN, error) {
	files, err := ioutil.ReadDir(d.cdnDirPath)
	if err != nil {
		return nil, err
	}

	var data []*CDN
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

		var s *CDN
		if err := json.Unmarshal(b, &s); err != nil {
			return nil, err
		}

		data = append(data, s)
	}

	return data, nil
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

type managerClient struct {
	managerclient.Client
	SchedulerClusterID uint
}

func NewManagerClient(client managerclient.Client, schedulerClusterID uint) dc.ManagerClient {
	return &managerClient{
		Client:             client,
		SchedulerClusterID: schedulerClusterID,
	}
}

func (mc *managerClient) Get() (interface{}, error) {
	scheduler, err := mc.GetScheduler(&manager.GetSchedulerRequest{
		HostName:           iputils.HostName,
		SourceType:         manager.SourceType_SCHEDULER_SOURCE,
		SchedulerClusterId: uint64(mc.SchedulerClusterID),
	})
	if err != nil {
		return nil, err
	}

	return scheduler, nil
}
