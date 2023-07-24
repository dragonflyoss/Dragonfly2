/*
 *     Copyright 2023 The Dragonfly Authors
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

	"google.golang.org/protobuf/types/known/emptypb"

	commonv1 "d7y.io/api/v2/pkg/apis/common/v1"
	schedulerv1 "d7y.io/api/v2/pkg/apis/scheduler/v1"

	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/types"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/metrics"
	"d7y.io/dragonfly/v2/scheduler/networktopology"
	"d7y.io/dragonfly/v2/scheduler/resource"
	"d7y.io/dragonfly/v2/scheduler/scheduling"
	"d7y.io/dragonfly/v2/scheduler/service"
	"d7y.io/dragonfly/v2/scheduler/storage"
)

// schedulerServerV1 is v1 version of the scheduler grpc server.
type schedulerServerV1 struct {
	// Service interface.
	service *service.V1
}

// newSchedulerServerV1 returns v1 version of the scheduler server.
func newSchedulerServerV1(
	cfg *config.Config,
	resource resource.Resource,
	scheduling scheduling.Scheduling,
	dynconfig config.DynconfigInterface,
	storage storage.Storage,
	networkTopology networktopology.NetworkTopology,
) schedulerv1.SchedulerServer {
	return &schedulerServerV1{service.NewV1(cfg, resource, scheduling, dynconfig, storage, networkTopology)}
}

// RegisterPeerTask registers peer and triggers seed peer download task.
func (s *schedulerServerV1) RegisterPeerTask(ctx context.Context, req *schedulerv1.PeerTaskRequest) (*schedulerv1.RegisterResult, error) {
	// FIXME Scheudler will not generate task id.
	if req.TaskId == "" {
		req.TaskId = idgen.TaskIDV1(req.Url, req.UrlMeta)
	}

	tag := req.UrlMeta.Tag
	application := req.UrlMeta.Application
	priority := req.UrlMeta.Priority

	// Collect RegisterPeerCount metrics.
	metrics.RegisterPeerCount.WithLabelValues(priority.String(), commonv1.TaskType_Normal.String(),
		tag, application, types.HostTypeNormalName).Inc()
	resp, err := s.service.RegisterPeerTask(ctx, req)
	if err != nil {
		// Collect RegisterPeerFailureCount metrics.
		metrics.RegisterPeerFailureCount.WithLabelValues(priority.String(), commonv1.TaskType_Normal.String(),
			tag, application, types.HostTypeNormalName).Inc()
	}

	return resp, err
}

// ReportPieceResult handles the piece information reported by dfdaemon.
func (s *schedulerServerV1) ReportPieceResult(stream schedulerv1.Scheduler_ReportPieceResultServer) error {
	// Collect ConcurrentScheduleGauge metrics.
	metrics.ConcurrentScheduleGauge.Inc()
	defer metrics.ConcurrentScheduleGauge.Dec()

	return s.service.ReportPieceResult(stream)
}

// ReportPeerResult handles peer result reported by dfdaemon.
func (s *schedulerServerV1) ReportPeerResult(ctx context.Context, req *schedulerv1.PeerResult) (*emptypb.Empty, error) {
	return new(emptypb.Empty), s.service.ReportPeerResult(ctx, req)
}

// AnnounceTask informs scheduler a peer has completed task.
func (s *schedulerServerV1) AnnounceTask(ctx context.Context, req *schedulerv1.AnnounceTaskRequest) (*emptypb.Empty, error) {
	// Collect AnnouncePeerCount metrics.
	metrics.AnnouncePeerCount.Inc()
	if err := s.service.AnnounceTask(ctx, req); err != nil {
		// Collect AnnouncePeerFailureCount metrics.
		metrics.AnnouncePeerFailureCount.Inc()
		return nil, err
	}

	return new(emptypb.Empty), nil
}

// StatTask checks if the given task exists.
func (s *schedulerServerV1) StatTask(ctx context.Context, req *schedulerv1.StatTaskRequest) (*schedulerv1.Task, error) {
	// Collect StatTaskCount metrics.
	metrics.StatTaskCount.Inc()
	resp, err := s.service.StatTask(ctx, req)
	if err != nil {
		// Collect StatTaskFailureCount metrics.
		metrics.StatTaskFailureCount.Inc()
		return nil, err
	}

	return resp, nil
}

// LeaveTask makes the peer unschedulable.
func (s *schedulerServerV1) LeaveTask(ctx context.Context, req *schedulerv1.PeerTarget) (*emptypb.Empty, error) {
	// Collect LeavePeerCount metrics.
	metrics.LeavePeerCount.Inc()
	if err := s.service.LeaveTask(ctx, req); err != nil {
		// Collect LeavePeerFailureCount metrics.
		metrics.LeavePeerFailureCount.Inc()
		return nil, err
	}

	return new(emptypb.Empty), nil
}

// AnnounceHost announces host to scheduler.
func (s *schedulerServerV1) AnnounceHost(ctx context.Context, req *schedulerv1.AnnounceHostRequest) (*emptypb.Empty, error) {
	// Collect AnnounceHostCount metrics.
	metrics.AnnounceHostCount.WithLabelValues(req.Os, req.Platform, req.PlatformFamily, req.PlatformVersion,
		req.KernelVersion, req.Build.GitVersion, req.Build.GitCommit, req.Build.GoVersion, req.Build.Platform).Inc()
	if err := s.service.AnnounceHost(ctx, req); err != nil {
		// Collect AnnounceHostFailureCount metrics.
		metrics.AnnounceHostFailureCount.WithLabelValues(req.Os, req.Platform, req.PlatformFamily, req.PlatformVersion,
			req.KernelVersion, req.Build.GitVersion, req.Build.GitCommit, req.Build.GoVersion, req.Build.Platform).Inc()
		return nil, err
	}

	return new(emptypb.Empty), nil
}

// LeaveHost releases host in scheduler.
func (s *schedulerServerV1) LeaveHost(ctx context.Context, req *schedulerv1.LeaveHostRequest) (*emptypb.Empty, error) {
	// Collect LeaveHostCount metrics.
	metrics.LeaveHostCount.Inc()
	if err := s.service.LeaveHost(ctx, req); err != nil {
		// Collect LeaveHostFailureCount metrics.
		metrics.LeaveHostFailureCount.Inc()
		return nil, err
	}

	return new(emptypb.Empty), nil
}

// SyncProbes sync probes of the host.
func (s *schedulerServerV1) SyncProbes(stream schedulerv1.Scheduler_SyncProbesServer) error {
	// Collect SyncProbesCount metrics.
	metrics.SyncProbesCount.Inc()
	if err := s.service.SyncProbes(stream); err != nil {
		// Collect SyncProbesFailureCount metrics.
		metrics.SyncProbesFailureCount.Inc()
		return err
	}

	return nil
}
