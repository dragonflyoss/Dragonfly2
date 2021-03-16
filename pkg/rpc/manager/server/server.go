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
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/manager"
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
	AddConfig(context.Context, *manager.AddConfigRequest) (*manager.AddConfigResponse, error)
	DeleteConfig(context.Context, *manager.DeleteConfigRequest) (*manager.DeleteConfigResponse, error)
	UpdateConfig(context.Context, *manager.UpdateConfigRequest) (*manager.UpdateConfigResponse, error)
	GetConfig(context.Context, *manager.GetConfigRequest) (*manager.GetConfigResponse, error)
	ListConfigs(context.Context, *manager.ListConfigsRequest) (*manager.ListConfigsResponse, error)
	KeepAlive(context.Context, *manager.KeepAliveRequest) (*manager.KeepAliveResponse, error)
	ListSchedulers(context.Context, *manager.ListSchedulersRequest) (*manager.ListSchedulersResponse, error)
}

func (p *proxy) AddConfig(ctx context.Context, req *manager.AddConfigRequest) (*manager.AddConfigResponse, error) {
	return p.server.AddConfig(ctx, req)
}

func (p *proxy) DeleteConfig(ctx context.Context, req *manager.DeleteConfigRequest) (*manager.DeleteConfigResponse, error) {
	return p.server.DeleteConfig(ctx, req)
}

func (p *proxy) UpdateConfig(ctx context.Context, req *manager.UpdateConfigRequest) (*manager.UpdateConfigResponse, error) {
	return p.server.UpdateConfig(ctx, req)
}

func (p *proxy) GetConfig(ctx context.Context, req *manager.GetConfigRequest) (*manager.GetConfigResponse, error) {
	return p.server.GetConfig(ctx, req)
}

func (p *proxy) ListConfigs(ctx context.Context, req *manager.ListConfigsRequest) (*manager.ListConfigsResponse, error) {
	return p.server.ListConfigs(ctx, req)
}

func (p *proxy) KeepAlive(ctx context.Context, req *manager.KeepAliveRequest) (*manager.KeepAliveResponse, error) {
	return p.server.KeepAlive(ctx, req)
}

func (p *proxy) ListSchedulers(ctx context.Context, req *manager.ListSchedulersRequest) (*manager.ListSchedulersResponse, error) {
	return p.server.ListSchedulers(ctx, req)
}
