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
	"github.com/dragonflyoss/Dragonfly2/pkg/basic"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/manager"
	"github.com/dragonflyoss/Dragonfly2/pkg/safe"
	"google.golang.org/grpc"
)

func init() {
	logDir := "/var/log/dragonfly"

	bizLogger := logger.CreateLogger(logDir+"/managerClient.log", 300, -1, -1, false, false)
	log := bizLogger.Sugar()
	logger.SetBizLogger(log)
	logger.SetGrpcLogger(log)
}

// ManagerClient
type ManagerClient interface {
	// GetSchedulers
	GetSchedulers(context.Context, *manager.SchedulerNodeRequest, ...grpc.CallOption) (*manager.SchedulerNodes, error)
	// GetCdnNodes
	GetCdnNodes(context.Context, *manager.CdnNodeRequest, ...grpc.CallOption) (*manager.CdnNodes, error)
	// KeepAlive
	KeepAlive(context.Context, ...grpc.CallOption) (<-chan *manager.HeartRequest, chan<- *manager.ManagementConfig, error)
	// close the conn
	Close() error
}

type managerClient struct {
	*rpc.Connection
	Client manager.ManagerClient
}

// init client info excepting connection
var initClientFunc = func(c *rpc.Connection) {
	dc := c.Ref.(*managerClient)
	dc.Client = manager.NewManagerClient(c.Conn)
	dc.Connection = c
}

func CreateClient(netAddrs []basic.NetAddr) (ManagerClient, error) {
	if client, err := rpc.BuildClient(&managerClient{}, initClientFunc, netAddrs); err != nil {
		return nil, err
	} else {
		return client.(*managerClient), nil
	}
}

func (dc *managerClient) GetSchedulers(ctx context.Context, req *manager.SchedulerNodeRequest, opts ...grpc.CallOption) (shs *manager.SchedulerNodes, err error) {
	xc, _, nextNum := dc.GetClientSafely()
	client := xc.(manager.ManagerClient)

	res, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		return client.GetSchedulers(ctx, req, opts...)
	}, 0.5, 5.0, 5)

	if err == nil {
		shs = res.(*manager.SchedulerNodes)
	}

	if err != nil {
		if err = dc.TryMigrate(nextNum, err); err == nil {
			return dc.GetSchedulers(ctx, req, opts...)
		}
	}
	return
}

func (dc *managerClient) GetCdnNodes(ctx context.Context, req *manager.CdnNodeRequest, opts ...grpc.CallOption) (chs *manager.CdnNodes, err error) {
	mc, _, nextNum := dc.GetClientSafely()
	client := mc.(manager.ManagerClient)
	res, err := rpc.ExecuteWithRetry(func() (interface{}, error) {
		return client.GetCdnNodes(ctx, req, opts...)
	}, 0.5, 5.0, 5)

	if err == nil {
		chs = res.(*manager.CdnNodes)
	}
	if err != nil {
		if err = dc.TryMigrate(nextNum, err); err == nil {
			return dc.GetCdnNodes(ctx, req, opts...)
		}
	}
	return
}

func (dc *managerClient) KeepAlive(ctx context.Context, opts ...grpc.CallOption) (<-chan *manager.HeartRequest, chan<- *manager.ManagementConfig, error) {
	heartReqChan := make(<-chan *manager.HeartRequest, 4)
	configChan := make(chan<- *manager.ManagementConfig, 4)

	pts, err := newConfigStream(dc, ctx, opts)
	if err != nil {
		return nil, nil, err
	}

	go send(pts, heartReqChan)

	go receive(pts, configChan)

	return heartReqChan, configChan, nil
}

func send(cs *configStream, prc <-chan *manager.HeartRequest) {
	safe.Call(func() {
		defer cs.closeSend()
		for v := range prc {
			_ = cs.send(v)
		}
	})
}

func receive(cs *configStream, mcc chan<- *manager.ManagementConfig) {
	safe.Call(func() {
		defer close(mcc)
		for {
			config, err := cs.recv()
			if err == nil {
				mcc <- config
			} else {
				mcc <- base.NewResWithErr(config, err).(*manager.ManagementConfig)
				return
			}
		}
	})
}
