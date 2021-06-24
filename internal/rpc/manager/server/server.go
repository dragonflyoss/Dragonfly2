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

// ManagerServer interface
type ManagerServer interface {
	GetCDN(context.Context, *manager.GetCDNRequest) (*manager.CDN, error)

	GetScheduler(context.Context, *manager.GetSchedulersRequest) (*manager.Scheduler, error)

	ListSchedulers(context.Context, *manager.ListSchedulersRequest) (*manager.ListSchedulersResponse, error)

	KeepAlive(context.Context, *manager.KeepAliveRequest) (*empty.Empty, error)
}

func (p *proxy) GetCDN(ctx context.Context, req *manager.GetCDNRequest) (*manager.CDN, error) {
	return p.server.GetCDN(ctx, req)
}

func (p *proxy) GetScheduler(ctx context.Context, req *manager.GetSchedulersRequest) (*manager.Scheduler, error) {
	return p.server.GetScheduler(ctx, req)
}

func (p *proxy) ListSchedulers(ctx context.Context, req *manager.ListSchedulersRequest) (*manager.ListSchedulersResponse, error) {
	return p.server.ListSchedulers(ctx, req)
}

func (p *proxy) KeepAlive(ctx context.Context, req *manager.KeepAliveRequest) (*empty.Empty, error) {
	return p.server.KeepAlive(ctx, req)
}
