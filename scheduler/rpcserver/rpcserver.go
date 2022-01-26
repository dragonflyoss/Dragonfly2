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

	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"

	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/scheduler/metrics"
	"d7y.io/dragonfly/v2/scheduler/service"
)

type Server struct {
	service service.Service
	scheduler.UnimplementedSchedulerServer
}

// New returns a new transparent scheduler server from the given options
func New(service service.Service, opts ...grpc.ServerOption) *grpc.Server {
	svr := &Server{service: service}
	grpcServer := grpc.NewServer(append(rpc.DefaultServerOptions, opts...)...)
	scheduler.RegisterSchedulerServer(grpcServer, svr)
	return grpcServer
}

// RegisterPeerTask registers peer and triggers CDN download task
func (s *Server) RegisterPeerTask(ctx context.Context, req *scheduler.PeerTaskRequest) (*scheduler.RegisterResult, error) {
	metrics.RegisterPeerTaskCount.Inc()

	resp, err := s.service.RegisterPeerTask(ctx, req)
	if err != nil {
		metrics.RegisterPeerTaskFailureCount.Inc()
	} else {
		metrics.PeerTaskCounter.WithLabelValues(resp.SizeScope.String()).Inc()
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
	metrics.DownloadCount.Inc()
	if req.Success {
		metrics.P2PTraffic.Add(float64(req.Traffic))
		metrics.PeerTaskDownloadDuration.Observe(float64(req.Cost))
	} else {
		metrics.DownloadFailureCount.Inc()
	}

	return new(empty.Empty), s.service.ReportPeerResult(ctx, req)
}

// LeaveTask makes the peer unschedulable
func (s *Server) LeaveTask(ctx context.Context, req *scheduler.PeerTarget) (*empty.Empty, error) {
	return new(empty.Empty), s.service.LeaveTask(ctx, req)
}
