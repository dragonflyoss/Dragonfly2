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
	"path/filepath"
	"time"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	dc "d7y.io/dragonfly/v2/internal/dynconfig"
)

var (
	// Cache filename
	cacheFileName = "cdn_dynconfig"

	// Notify observer interval
	watchInterval = 10 * time.Second
)

// todo move this interface to internal/dynconfig
type DynconfigInterface interface {
	// Get the dynamic config from manager.
	Get() (interface{}, error)

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
	OnNotify(interface{})
}

type dynConfig struct {
	ds        *dc.Dynconfig
	observers map[Observer]struct{}
	done      chan bool
}

func NewDynconfig(cfg *DynConfig, drawFunc func() (interface{}, error)) (DynconfigInterface, error) {
	d := &dynConfig{
		done: make(chan bool),
	}

	ds, err := dc.New(
		cfg.SourceType,
		dc.WithCachePath(filepath.Join(cfg.CachePath, cacheFileName)),
		dc.WithExpireTime(cfg.RefreshInterval),
		dc.WithManagerClient(newManagerClient(drawFunc)),
	)
	if err != nil {
		return nil, err
	}

	d.ds = ds
	d.Serve()
	return d, nil
}

func (d *dynConfig) Register(l Observer) {
	d.observers[l] = struct{}{}
}

func (d *dynConfig) Deregister(l Observer) {
	delete(d.observers, l)
}

func (d *dynConfig) Get() (interface{}, error) {
	return d.ds.Get()
}

func (d *dynConfig) Notify() error {
	config, err := d.ds.Get()
	if err != nil {
		return err
	}

	for o := range d.observers {
		o.OnNotify(config)
	}

	return nil
}

func (d *dynConfig) Serve() error {
	if err := d.Notify(); err != nil {
		return err
	}

	go d.watch()
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

func (d *dynConfig) Stop() error {
	close(d.done)
	return d.ds.Clean()
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
