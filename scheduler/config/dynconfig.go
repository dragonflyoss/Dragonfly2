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
	"time"

	dc "d7y.io/dragonfly/v2/pkg/dynconfig"
	"d7y.io/dragonfly/v2/pkg/rpc/manager"
	"d7y.io/dragonfly/v2/pkg/rpc/manager/client"
	"d7y.io/dragonfly/v2/pkg/util/net/iputils"
)

type DynconfigInterface interface {
	Get() (*manager.SchedulerConfig, error)
}

type dynconfig struct {
	*dc.Dynconfig
}

func NewDynconfig(rawClient client.ManagerClient, expire time.Duration) (DynconfigInterface, error) {
	client, err := dc.New(dc.ManagerSourceType, expire, dc.WithManagerClient(newManagerClient(rawClient)))
	if err != nil {
		return nil, err
	}

	return &dynconfig{client}, nil
}

func (d *dynconfig) Get() (*manager.SchedulerConfig, error) {
	var config *manager.SchedulerConfig
	if err := d.Unmarshal(config); err != nil {
		return nil, err
	}

	return config, nil
}

type managerClient struct {
	client.ManagerClient
}

func newManagerClient(client client.ManagerClient) dc.ManagerClient {
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
