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

//go:generate mockgen -destination ./mocks/mock_dynconfig.go -package mocks d7y.io/dragonfly/v2/cdn/dynconfig DynconfigInterface

package dynconfig

import (
	"reflect"
	"sync"
	"time"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	dc "d7y.io/dragonfly/v2/internal/dynconfig"
)

var (
	// Cache filename
	watchInterval = 10 * time.Second
)

// todo move this interface to internal/dynconfig
type Interface interface {
	// Get the dynamic config from configServer or local file.
	Get(dest interface{}) error

	// Register allows an instance to register itself to listen/observe events.
	Register(Observer)

	// Deregister allows an instance to remove itself from the collection of observers/listeners.
	Deregister(Observer)

	// Notify publishes new events to listeners.
	Notify() error

	// Stop the dynconfig listening service.
	Stop()
}

type Observer interface {
	// OnNotify allows an event to be "published" to interface implementations.
	OnNotify(interface{})
}

type dynConfig struct {
	ds *dc.Dynconfig

	observers map[Observer]struct{}
	data      interface{}
	stopOnce  sync.Once
	done      chan struct{}
}

func NewDynconfig(cfg Config, drawFunc func() (interface{}, error)) (Interface, error) {
	ds, err := dc.New(
		cfg.SourceType,
		dc.WithCachePath(cfg.CachePath),
		dc.WithExpireTime(cfg.RefreshInterval),
		dc.WithLocalConfigPath(cfg.CachePath),
		dc.WithManagerClient(newManagerClient(drawFunc)),
	)
	if err != nil {
		return nil, err
	}
	configData, err := ds.Get()
	if err != nil {
		return nil, err
	}
	if cfg.RefreshInterval < watchInterval {
		watchInterval = cfg.RefreshInterval
	}
	d := &dynConfig{
		ds:        ds,
		observers: map[Observer]struct{}{},
		data:      configData,
		done:      make(chan struct{}),
	}
	go d.watch()
	return d, nil
}

func (d *dynConfig) Register(l Observer) {
	d.observers[l] = struct{}{}
	l.OnNotify(d.data)
}

func (d *dynConfig) Deregister(l Observer) {
	delete(d.observers, l)
}

func (d *dynConfig) Get(dest interface{}) error {
	return d.ds.Unmarshal(dest)
}

func (d *dynConfig) Notify() error {
	config, err := d.ds.Get()
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(config, d.data) {
		d.data = config
		for o := range d.observers {
			o.OnNotify(config)
		}
	}
	return nil
}

func (d *dynConfig) watch() {
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

func (d *dynConfig) Stop() {
	d.stopOnce.Do(func() {
		close(d.done)
		if err := d.ds.Clean(); err != nil {
			logger.Errorf("clean dataSource failed", err)
		}
	})
}

type managerClient struct {
	drawFunc func() (interface{}, error)
}

func newManagerClient(drawFunc func() (interface{}, error)) dc.ManagerClient {
	return &managerClient{
		drawFunc: drawFunc,
	}
}

func (mc *managerClient) Get() (interface{}, error) {
	return mc.drawFunc()
}
