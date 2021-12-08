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
	"strconv"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	grpc_validator "github.com/grpc-ecosystem/go-grpc-middleware/validator"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/manager/metrics"
	"d7y.io/dragonfly/v2/pkg/rpc/manager"
)

var defaultStreamMiddleWares = []grpc.StreamServerInterceptor{
	grpc_validator.StreamServerInterceptor(),
	grpc_recovery.StreamServerInterceptor(),
	grpc_prometheus.StreamServerInterceptor,
	grpc_zap.StreamServerInterceptor(logger.GrpcLogger.Desugar()),
}

var defaultUnaryMiddleWares = []grpc.UnaryServerInterceptor{
	grpc_validator.UnaryServerInterceptor(),
	grpc_recovery.UnaryServerInterceptor(),
	grpc_prometheus.UnaryServerInterceptor,
	grpc_zap.UnaryServerInterceptor(logger.GrpcLogger.Desugar()),
}

// ManagerServer is the server API for Manager service.
type ManagerServer interface {
	// Get CDN and CDN cluster configuration
	GetCDN(context.Context, *manager.GetCDNRequest) (*manager.CDN, error)
	// Update CDN configuration
	UpdateCDN(context.Context, *manager.UpdateCDNRequest) (*manager.CDN, error)
	// Get Scheduler and Scheduler cluster configuration
	GetScheduler(context.Context, *manager.GetSchedulerRequest) (*manager.Scheduler, error)
	// Update scheduler configuration
	UpdateScheduler(context.Context, *manager.UpdateSchedulerRequest) (*manager.Scheduler, error)
	// List acitve schedulers configuration
	ListSchedulers(context.Context, *manager.ListSchedulersRequest) (*manager.ListSchedulersResponse, error)
	// KeepAlive with manager
	KeepAlive(manager.Manager_KeepAliveServer) error
}

type proxy struct {
	server ManagerServer
	manager.UnimplementedManagerServer
}

func New(managerServer ManagerServer, jaeger bool, opts ...grpc.ServerOption) *grpc.Server {
	if jaeger {
		defaultStreamMiddleWares = append(defaultStreamMiddleWares, otelgrpc.StreamServerInterceptor())
		defaultUnaryMiddleWares = append(defaultUnaryMiddleWares, otelgrpc.UnaryServerInterceptor())
	}

	grpcServer := grpc.NewServer(append([]grpc.ServerOption{
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(defaultStreamMiddleWares...)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(defaultUnaryMiddleWares...)),
	}, opts...)...)

	manager.RegisterManagerServer(grpcServer, &proxy{server: managerServer})
	return grpcServer
}

func (p *proxy) GetCDN(ctx context.Context, req *manager.GetCDNRequest) (*manager.CDN, error) {
	return p.server.GetCDN(ctx, req)
}

func (p *proxy) UpdateCDN(ctx context.Context, req *manager.UpdateCDNRequest) (*manager.CDN, error) {
	return p.server.UpdateCDN(ctx, req)
}

func (p *proxy) GetScheduler(ctx context.Context, req *manager.GetSchedulerRequest) (*manager.Scheduler, error) {
	return p.server.GetScheduler(ctx, req)
}

func (p *proxy) UpdateScheduler(ctx context.Context, req *manager.UpdateSchedulerRequest) (*manager.Scheduler, error) {
	return p.server.UpdateScheduler(ctx, req)
}

func (p *proxy) ListSchedulers(ctx context.Context, req *manager.ListSchedulersRequest) (*manager.ListSchedulersResponse, error) {
	resp, err := p.server.ListSchedulers(ctx, req)
	if err != nil {
		return nil, err
	}

	metrics.SchedulerClusterUsageCount.WithLabelValues(
		strconv.Itoa(int(resp.SchedulerCluster.Id)),
		resp.SchedulerCluster.Name,
		req.HostName,
		req.Ip,
	).Add(float64(1))

	return resp, nil
}

func (p *proxy) KeepAlive(req manager.Manager_KeepAliveServer) error {
	return p.server.KeepAlive(req)
}
