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

package server

import (
	"context"

	"d7y.io/dragonfly/v2/internal/rpc"
	"d7y.io/dragonfly/v2/internal/rpc/manager"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
)

func init() {
	// set register with server implementation.
	rpc.SetRegister(func(s *grpc.Server, impl interface{}) {
		manager.RegisterManagerServer(s, &proxy{server: impl.(ManagerServer)})
	})
}

type proxy struct {
	server ManagerServer
	manager.UnimplementedManagerServer
}

// see manager.ManagerServer
type ManagerServer interface {
	GetSchedulers(context.Context, *manager.GetSchedulersRequest) (*manager.SchedulerNodes, error)

	KeepAlive(context.Context, *manager.KeepAliveRequest) error

	GetClusterConfig(ctx context.Context, req *manager.GetClusterConfigRequest) (*manager.ClusterConfig, error)
}

func (p *proxy) GetSchedulers(ctx context.Context, req *manager.GetSchedulersRequest) (*manager.SchedulerNodes, error) {
	return p.server.GetSchedulers(ctx, req)
}

func (p *proxy) KeepAlive(ctx context.Context, req *manager.KeepAliveRequest) (*empty.Empty, error) {
	return new(empty.Empty), p.server.KeepAlive(ctx, req)
}

func (p *proxy) GetClusterConfig(ctx context.Context, req *manager.GetClusterConfigRequest) (*manager.ClusterConfig, error) {
	return p.server.GetClusterConfig(ctx, req)
}
