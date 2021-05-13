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

	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	"d7y.io/dragonfly/v2/pkg/dynconfig"
	"d7y.io/dragonfly/v2/pkg/rpc/manager"
	"d7y.io/dragonfly/v2/pkg/rpc/manager/client"
	"d7y.io/dragonfly/v2/pkg/util/net/iputils"
)

type DynconfigInterface interface {
}

type Dynconfig struct {
	client dynconfig.Dynconfig
}

func NewDynconfig(cfg ManagerConfig) (Dynconfig, error) {
	managerClient, err := newManagerClient(cfg.NetAddrs)
	if err != nil {
		return nil, err
	}

	client, err := dynconfig.New(dynconfig.ManagerSourceType, cfg.Expire, dynconfig.WithManagerClient(managerClient))
	if err != nil {
		return nil, err
	}

	return &dynconfig{
		client: client,
	}, nil
}

func (d *dynconfig) Get() dynconfig {
	return d
}

type managerClient struct {
	client client.ManagerClient
}

func newManagerClient(addrs []dfnet.NetAddr) (dynconfig.ManagerClient, error) {
	client, err := client.NewClient(addrs)
	if err != nil {
		return nil, err
	}
	return &managerClient{client: client}, nil
}

func (mc *managerClient) Get() (interface{}, error) {
	scConfig, err := mc.client.GetSchedulerClusterConfig(context.Background(), &manager.GetClusterConfigRequest{
		HostName: iputils.HostName,
		Type:     manager.ResourceType_Scheduler,
	})
	if err != nil {
		return nil, err
	}

	return scConfig.CdnHosts, nil
}
