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
	empty "google.golang.org/protobuf/types/known/emptypb"

	schedulerv1 "d7y.io/api/pkg/apis/scheduler/v1"

	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler/server"
	"d7y.io/dragonfly/v2/scheduler/metrics"
	"d7y.io/dragonfly/v2/scheduler/resource"
	"d7y.io/dragonfly/v2/scheduler/service"
)

// Server is grpc server.
type Server struct {
	// Service interface.
	service *service.Service
}

// New returns a new transparent scheduler server from the given options.
func New(service *service.Service, opts ...grpc.ServerOption) *grpc.Server {
	return server.New(&Server{service: service}, opts...)
}

// RegisterPeerTask registers peer and triggers seed peer download task.
func (s *Server) RegisterPeerTask(ctx context.Context, req *schedulerv1.PeerTaskRequest) (*schedulerv1.RegisterResult, error) {
	// FIXME Scheudler will not generate task id.
	if req.TaskId == "" {
		req.TaskId = idgen.TaskID(req.Url, req.UrlMeta)
	}

	tag := resource.DefaultTag
	if req.UrlMeta.Tag != "" {
		tag = req.UrlMeta.Tag
	}

	application := resource.DefaultApplication
	if req.UrlMeta.Application != "" {
		application = req.UrlMeta.Application
	}
	metrics.RegisterPeerTaskCount.WithLabelValues(tag, application).Inc()

	resp, err := s.service.RegisterPeerTask(ctx, req)
	if err != nil {
		metrics.RegisterPeerTaskFailureCount.WithLabelValues(tag, application).Inc()
	} else {
		metrics.PeerTaskCounter.WithLabelValues(tag, application, resp.SizeScope.String()).Inc()
	}

	return resp, err
}

// ReportPieceResult handles the piece information reported by dfdaemon.
func (s *Server) ReportPieceResult(stream schedulerv1.Scheduler_ReportPieceResultServer) error {
	metrics.ConcurrentScheduleGauge.Inc()
	defer metrics.ConcurrentScheduleGauge.Dec()

	return s.service.ReportPieceResult(stream)
}

// ReportPeerResult handles peer result reported by dfdaemon.
func (s *Server) ReportPeerResult(ctx context.Context, req *schedulerv1.PeerResult) (*empty.Empty, error) {
	return new(empty.Empty), s.service.ReportPeerResult(ctx, req)
}

// AnnounceTask informs scheduler a peer has completed task.
func (s *Server) AnnounceTask(ctx context.Context, req *schedulerv1.AnnounceTaskRequest) (*empty.Empty, error) {
	metrics.AnnounceTaskCount.Inc()
	if err := s.service.AnnounceTask(ctx, req); err != nil {
		metrics.AnnounceTaskFailureCount.Inc()
		return new(empty.Empty), err
	}

	return new(empty.Empty), nil
}

// StatTask checks if the given task exists.
func (s *Server) StatTask(ctx context.Context, req *schedulerv1.StatTaskRequest) (*schedulerv1.Task, error) {
	metrics.StatTaskCount.Inc()
	task, err := s.service.StatTask(ctx, req)
	if err != nil {
		metrics.StatTaskFailureCount.Inc()
		return nil, err
	}

	return task, nil
}

// LeaveTask makes the peer unschedulable.
func (s *Server) LeaveTask(ctx context.Context, req *schedulerv1.PeerTarget) (*empty.Empty, error) {
	return new(empty.Empty), s.service.LeaveTask(ctx, req)
}

// AnnounceHost announces host to scheduler.
func (s *Server) AnnounceHost(ctx context.Context, req *schedulerv1.AnnounceHostRequest) (*empty.Empty, error) {
	metrics.AnnounceHostCount.WithLabelValues(req.Hostname, req.Ip, req.Os, req.Platform, req.PlatformFamily, req.PlatformVersion,
		req.KernelVersion, req.Build.GitVersion, req.Build.GitCommit, req.Build.GoVersion, req.Build.Platform).Inc()
	if err := s.service.AnnounceHost(ctx, req); err != nil {
		metrics.AnnounceHostFailureCount.Inc()
		return new(empty.Empty), err
	}

	return new(empty.Empty), nil
}

// LeaveHost releases host in scheduler.
func (s *Server) LeaveHost(ctx context.Context, req *schedulerv1.LeaveHostRequest) (*empty.Empty, error) {
	metrics.LeaveHostCount.Inc()
	if err := s.service.LeaveHost(ctx, req); err != nil {
		metrics.LeaveHostFailureCount.Inc()
		return new(empty.Empty), err
	}

	return new(empty.Empty), nil
}
