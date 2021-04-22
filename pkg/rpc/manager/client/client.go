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
	"sync"
	"time"

	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/manager"
	"google.golang.org/grpc"
)

var mc *managerClient

var once sync.Once

func GetClient(addrs ...dfnet.NetAddr) (ManagerClient, error) {
	once.Do(func() {
		opts := make([]grpc.DialOption, 0, 0)
		opts = append(opts, grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`))
		mc = &managerClient{
			Connection: rpc.NewConnection(context.Background(), "manager", make([]dfnet.NetAddr, 0), []rpc.ConnOption{
				rpc.WithConnExpireTime(10 * time.Second),
				rpc.WithDialOption(opts),
			}),
		}
	})
	if len(addrs) == 0 {
		addrs = []dfnet.NetAddr{
			{
				Type: dfnet.TCP,
				Addr: "127.0.0.1:8004",
			},
		}
	}
	err := mc.Connection.AddServerNodes(addrs)
	if err != nil {
		return nil, err
	}
	return mc, nil
}

type wrapperManagerClient struct {
	targetName     string
	targetInstance manager.ManagerClient
}

func (mc *managerClient) getManagerClient(key string) (*wrapperManagerClient, error) {
	clientConn, err := mc.Connection.GetClientConn(key, true)
	if err != nil {
		return nil, err
	}
	return &wrapperManagerClient{
		targetName:     clientConn.Target(),
		targetInstance: manager.NewManagerClient(clientConn)}, nil
}

// see manager.ManagerClient
type ManagerClient interface {
	// GetSchedulers
	GetSchedulers(ctx context.Context, opts ...grpc.CallOption) (*manager.SchedulerNodes, error)

	// KeepAlive keep alive for cdn or scheduler
	KeepAlive(ctx context.Context, req *KeepAliveRequest, opts ...grpc.CallOption) error

	// GetClusterConfig get cluster config for cdn or scheduler(client) from manager
	GetSchedulerClusterConfig(ctx context.Context, req *GetClusterConfigRequest, opts ...grpc.CallOption) (*manager.SchedulerConfig, error)

	// GetCdnClusterConfig
	GetCdnClusterConfig(ctx context.Context, req *GetClusterConfigRequest, opts ...grpc.CallOption) (*manager.CdnConfig, error)
}

// it is mutually exclusive between IsCdn and IsScheduler
type KeepAliveRequest struct {
	IsCdn       bool
	IsScheduler bool
	// keep alive interval(second), default is 3s
	Interval time.Duration
}

type GetClusterConfigRequest struct {
}

type managerClient struct {
	*rpc.Connection
}

func (mc *managerClient) GetSchedulers(ctx context.Context, opts ...grpc.CallOption) (*manager.SchedulerNodes, error) {
	panic("implement me")
}

func (mc *managerClient) KeepAlive(ctx context.Context, req *KeepAliveRequest, opts ...grpc.CallOption) error {
	panic("implement me")
}

func (mc *managerClient) GetSchedulerClusterConfig(ctx context.Context, req *GetClusterConfigRequest, opts ...grpc.CallOption) (*manager.SchedulerConfig, error) {
	panic("implement me")
}

func (mc *managerClient) GetCdnClusterConfig(ctx context.Context, req *GetClusterConfigRequest, opts ...grpc.CallOption) (*manager.CdnConfig, error) {
	panic("implement me")
}

func init() {
	var client *managerClient = nil
	var _ ManagerClient = client
}
