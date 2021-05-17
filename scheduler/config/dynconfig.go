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
	"reflect"
	"time"

	dc "d7y.io/dragonfly/v2/pkg/dynconfig"
	"d7y.io/dragonfly/v2/pkg/rpc/manager"
	"d7y.io/dragonfly/v2/pkg/rpc/manager/client"
	"d7y.io/dragonfly/v2/pkg/util/net/iputils"
)

type DynconfigInterface interface {
	// Get the dynamic config from manager
	Get() (*manager.SchedulerConfig, error)

	// Register allows an instance to register itself to listen/observe events.
	Register(Observer)

	// Deregister allows an instance to remove itself from the collection of observers/listeners.
	Deregister(Observer)

	// Notify publishes new events to listeners.
	Notify() error
}

type Observer interface {
	// OnNotify allows an event to be "published" to interface implementations.
	OnNotify(*manager.SchedulerConfig)
}

type dynconfig struct {
	*dc.Dynconfig
	observers map[Observer]struct{}
	data      *manager.SchedulerConfig
}

func NewDynconfig(sourceType dc.SourceType, expire time.Duration, options ...dc.Option) (DynconfigInterface, error) {
	client, err := dc.New(sourceType, expire, options...)
	if err != nil {
		return nil, err
	}

	d := &dynconfig{
		Dynconfig: client,
		observers: map[Observer]struct{}{},
	}

	return d, nil
}

func (d *dynconfig) Get() (*manager.SchedulerConfig, error) {
	var config *manager.SchedulerConfig
	if err := d.Unmarshal(&config); err != nil {
		return nil, err
	}

	d.data = config
	return config, nil
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

	if d.data == nil {
		d.data = config
	}

	if reflect.DeepEqual(d.data, config) {
		return nil
	}

	for o := range d.observers {
		o.OnNotify(config)
	}

	return nil
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
