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

package rpcserver

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	empty "google.golang.org/protobuf/types/known/emptypb"

	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/scheduler/metrics"
	"d7y.io/dragonfly/v2/scheduler/resource"
	"d7y.io/dragonfly/v2/scheduler/service"
)

// Server is grpc sercer
type Server struct {
	// Service interface
	service *service.Service

	// GRPC UnimplementedSchedulerServer interface
	scheduler.UnimplementedSchedulerServer
}

// New returns a new transparent scheduler server from the given options
func New(service *service.Service, opts ...grpc.ServerOption) *grpc.Server {
	svr := &Server{service: service}
	grpcServer := grpc.NewServer(append(rpc.DefaultServerOptions, opts...)...)

	// Register servers on grpc server
	scheduler.RegisterSchedulerServer(grpcServer, svr)
	healthpb.RegisterHealthServer(grpcServer, health.NewServer())
	return grpcServer
}

// RegisterPeerTask registers peer and triggers CDN download task
func (s *Server) RegisterPeerTask(ctx context.Context, req *scheduler.PeerTaskRequest) (*scheduler.RegisterResult, error) {
	bizTag := resource.DefaultBizTag
	if req.UrlMeta.Tag != "" {
		bizTag = req.UrlMeta.Tag
	}

	metrics.RegisterPeerTaskCount.WithLabelValues(bizTag).Inc()

	resp, err := s.service.RegisterPeerTask(ctx, req)
	if err != nil {
		metrics.RegisterPeerTaskFailureCount.WithLabelValues(bizTag).Inc()
	} else {
		metrics.PeerTaskCounter.WithLabelValues(bizTag, resp.SizeScope.String()).Inc()
	}

	return resp, err
}

// ReportPieceResult handles the piece information reported by dfdaemon
func (s *Server) ReportPieceResult(stream scheduler.Scheduler_ReportPieceResultServer) error {
	metrics.ConcurrentScheduleGauge.Inc()
	defer metrics.ConcurrentScheduleGauge.Dec()

	return s.service.ReportPieceResult(stream)
}

// ReportPeerResult handles peer result reported by dfdaemon
func (s *Server) ReportPeerResult(ctx context.Context, req *scheduler.PeerResult) (*empty.Empty, error) {
	return new(empty.Empty), s.service.ReportPeerResult(ctx, req)
}

// LeaveTask makes the peer unschedulable
func (s *Server) LeaveTask(ctx context.Context, req *scheduler.PeerTarget) (*empty.Empty, error) {
	return new(empty.Empty), s.service.LeaveTask(ctx, req)
}

// StatPeerTask checks if the given task exists in P2P network
func (s *Server) StatPeerTask(ctx context.Context, req *scheduler.StatPeerTaskRequest) (*base.GrpcDfResult, error) {
	// TODO: add metrics
	return s.service.StatPeerTask(ctx, req)
}
