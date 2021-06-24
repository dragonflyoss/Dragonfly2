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
	"fmt"
	"time"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/rpc"
	"d7y.io/dragonfly/v2/internal/rpc/manager"
	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
)

// ManagerClient interface
type ManagerClient interface {
	GetCDN(context.Context, *manager.GetCDNRequest) (*manager.CDN, error)

	GetScheduler(context.Context, *manager.GetSchedulerRequest) (*manager.Scheduler, error)

	ListSchedulers(context.Context, *manager.ListSchedulersRequest) (*manager.ListSchedulersResponse, error)

	KeepAlive(context.Context, *manager.KeepAliveRequest) (*empty.Empty, error)
}

type managerClient struct {
	client manager.ManagerClient
}

func New(addrs []dfnet.NetAddr, opts ...grpc.DialOption) (ManagerClient, error) {
	conn := rpc.NewConnection(context.Background(), "manager", addrs, []rpc.ConnOption{
		rpc.WithConnExpireTime(10 * time.Second),
		rpc.WithDialOption(opts),
	})

	for _, addr := range addrs {
		endpoint := addr.GetEndpoint()
		clientConn, err := conn.GetClientConnByTarget(endpoint)
		if err != nil {
			logger.Warnf("connect %s manager server failed: %v", endpoint, err)
			continue
		}

		logger.Infof("connect %s manager server successfully", endpoint)
		return &managerClient{client: manager.NewManagerClient(clientConn)}, nil
	}

	return nil, fmt.Errorf("all manager servers cannot be connected, server list: %s", addrs)
}

func (mc *managerClient) GetCDN(ctx context.Context, req *manager.GetCDNRequest) (*manager.CDN, error) {
	return mc.client.GetCDN(ctx, req)
}

func (mc *managerClient) GetScheduler(ctx context.Context, req *manager.GetSchedulerRequest) (*manager.Scheduler, error) {
	return mc.client.GetScheduler(ctx, req)
}

func (mc *managerClient) ListSchedulers(ctx context.Context, req *manager.ListSchedulersRequest) (*manager.ListSchedulersResponse, error) {
	return mc.client.ListSchedulers(ctx, req)
}

func (mc *managerClient) KeepAlive(ctx context.Context, req *manager.KeepAliveRequest) (*empty.Empty, error) {
	return mc.client.KeepAlive(ctx, req)
}
