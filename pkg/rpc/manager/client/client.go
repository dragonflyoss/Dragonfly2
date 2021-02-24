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

package client

import (
	"context"
	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/manager"
	"errors"
	"google.golang.org/grpc"
	"sync"
	"time"
)

func GetClientByAddrs(addrs dfnet.NetAddrs) (ManagerClient, error) {
	// user specify
	return newManagerClient(addrs)
}

func GetClientByAddr(connType dfnet.NetworkType, addrs ...string) (ManagerClient, error) {
	// user specify
	return newManagerClient(dfnet.NetAddrs{
		Type:  connType,
		Addrs: addrs,
	})
}

func newManagerClient(addrs dfnet.NetAddrs, opts ...grpc.DialOption) (ManagerClient, error) {
	if len(addrs.Addrs) == 0 {
		return nil, errors.New("address list of cdn is empty")
	}
	return &managerClient{
		Connection: rpc.NewConnection(addrs, opts...),
	}, nil
}

// see manager.ManagerClient
type ManagerClient interface {
	GetSchedulers(ctx context.Context, req *manager.NavigatorRequest, opts ...grpc.CallOption) (*manager.SchedulerNodes, error)
	// only call once
	KeepAlive(ctx context.Context, req *KeepAliveRequest, opts ...grpc.CallOption) error
	// GetLatestConfig return latest management config and cdn host map with host name key
	GetLatestConfig() (*manager.SchedulerConfig, map[string]*manager.ServerInfo, *manager.CdnConfig)
}

// it is mutually exclusive between IsCdn and IsScheduler
type KeepAliveRequest struct {
	IsCdn       bool
	IsScheduler bool
	// keep alive interval(second), default is 3s
	Interval time.Duration
}

type managerClient struct {
	*rpc.Connection
	Client manager.ManagerClient

	schedulerConfig *manager.SchedulerConfig
	cdnConfig       *manager.CdnConfig
	cdns            map[string]*manager.ServerInfo

	rwMutex   sync.RWMutex
	ch        chan struct{}
	closeDone bool
}

func (mc *managerClient) GetSchedulers(ctx context.Context, req *manager.NavigatorRequest, opts ...grpc.CallOption) (sns *manager.SchedulerNodes, err error) {
	res, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		return mc.Client.GetSchedulers(ctx, req, opts...)
	}, 0.5, 5.0, 5, nil)

	if err == nil {
		sns = res.(*manager.SchedulerNodes)
	}

	return
}

func (mc *managerClient) KeepAlive(ctx context.Context, req *KeepAliveRequest, opts ...grpc.CallOption) error {
	if (req.IsCdn && req.IsScheduler) || (!req.IsCdn && !req.IsScheduler) {
		return errors.New("IsCdn and IsScheduler must be exclusive")
	}

	if req.Interval <= 0 {
		req.Interval = 3 * time.Second
	} else if req.Interval > 30*time.Second {
		req.Interval = 30 * time.Second
	}

	hr := &manager.HeartRequest{
		HostName: dfnet.HostName,
	}
	if req.IsCdn {
		hr.From = &manager.HeartRequest_Cdn{Cdn: true}
	} else {
		hr.From = &manager.HeartRequest_Scheduler{Scheduler: true}
	}

	go func() {
		defer func() {
			if err := recover(); err != nil {
				logger.Errorf("keep alive exit:%v", err)
			}
		}()

		logger.Infof("trigger keep alive per %ds", req.Interval)

		for {
			config, err := mc.Client.KeepAlive(ctx, hr, opts...)
			if err == nil && config.State.Success {
				fillConfig(mc, config)
			} else {
				if err == nil {
					err = errors.New(config.State.Msg)
				}

				logger.Errorf("do keep alive error:%v", err)
			}

			time.Sleep(req.Interval)
		}
	}()

	return nil
}

func (mc *managerClient) GetLatestConfig() (*manager.SchedulerConfig, map[string]*manager.ServerInfo, *manager.CdnConfig) {
	if mc.schedulerConfig == nil && mc.cdnConfig == nil {
		<-mc.ch
	}

	mc.rwMutex.RLock()
	defer mc.rwMutex.RUnlock()
	return mc.schedulerConfig, mc.cdns, mc.cdnConfig
}

func fillConfig(mc *managerClient, config *manager.ManagementConfig) {
	mc.rwMutex.Lock()
	defer mc.rwMutex.Unlock()

	switch v := config.Config.(type) {
	case *manager.ManagementConfig_SchedulerConfig:
		if v.SchedulerConfig != nil {
			mc.schedulerConfig = v.SchedulerConfig
			if mc.schedulerConfig.CdnHosts != nil {
				cdns := make(map[string]*manager.ServerInfo)
				for _, one := range mc.schedulerConfig.CdnHosts {
					cdns[one.HostInfo.HostName] = one
				}
				mc.cdns = cdns
			}
		}
	case *manager.ManagementConfig_CdnConfig:
		if v.CdnConfig != nil {
			mc.cdnConfig = v.CdnConfig
		}
	default:
		break
	}

	if mc.schedulerConfig != nil || mc.cdnConfig != nil {
		if !mc.closeDone {
			close(mc.ch)
			mc.closeDone = true
		}
	}
}
