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

	emptypb "google.golang.org/protobuf/types/known/emptypb"

	commonv2 "d7y.io/api/v2/pkg/apis/common/v2"
	schedulerv2 "d7y.io/api/v2/pkg/apis/scheduler/v2"

	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/metrics"
	"d7y.io/dragonfly/v2/scheduler/networktopology"
	"d7y.io/dragonfly/v2/scheduler/resource"
	"d7y.io/dragonfly/v2/scheduler/scheduling"
	"d7y.io/dragonfly/v2/scheduler/service"
	"d7y.io/dragonfly/v2/scheduler/storage"
)

// TODO Implement v2 version of the rpc server apis.
// schedulerServerV2 is v2 version of the scheduler grpc server.
type schedulerServerV2 struct {
	// Service interface.
	service *service.V2
}

// newSchedulerServerV2 returns v2 version of the scheduler server.
func newSchedulerServerV2(
	cfg *config.Config,
	resource resource.Resource,
	scheduling scheduling.Scheduling,
	dynconfig config.DynconfigInterface,
	storage storage.Storage,
	networkTopology networktopology.NetworkTopology,
) schedulerv2.SchedulerServer {
	return &schedulerServerV2{service.NewV2(cfg, resource, scheduling, dynconfig, storage, networkTopology)}
}

// AnnouncePeer announces peer to scheduler.
func (s *schedulerServerV2) AnnouncePeer(stream schedulerv2.Scheduler_AnnouncePeerServer) error {
	// Collect ConcurrentScheduleGauge metrics.
	metrics.ConcurrentScheduleGauge.Inc()
	defer metrics.ConcurrentScheduleGauge.Dec()

	// Collect AnnouncePeerCount metrics.
	metrics.AnnouncePeerCount.Inc()
	if err := s.service.AnnouncePeer(stream); err != nil {
		// Collect AnnouncePeerFailureCount metrics.
		metrics.AnnouncePeerFailureCount.Inc()
		return err
	}

	return nil
}

// StatPeer checks information of peer.
func (s *schedulerServerV2) StatPeer(ctx context.Context, req *schedulerv2.StatPeerRequest) (*commonv2.Peer, error) {
	// Collect StatPeerCount metrics.
	metrics.StatPeerCount.Inc()
	resp, err := s.service.StatPeer(ctx, req)
	if err != nil {
		// Collect StatPeerFailureCount metrics.
		metrics.StatPeerFailureCount.Inc()
		return nil, err
	}

	return resp, nil
}

// LeavePeer releases peer in scheduler.
func (s *schedulerServerV2) LeavePeer(ctx context.Context, req *schedulerv2.LeavePeerRequest) (*emptypb.Empty, error) {
	// Collect LeavePeerCount metrics.
	metrics.LeavePeerCount.Inc()
	if err := s.service.LeavePeer(ctx, req); err != nil {
		// Collect LeavePeerFailureCount metrics.
		metrics.LeavePeerFailureCount.Inc()
		return nil, err
	}

	return new(emptypb.Empty), nil
}

// ExchangePeer exchanges peer information.
func (s *schedulerServerV2) ExchangePeer(ctx context.Context, req *schedulerv2.ExchangePeerRequest) (*schedulerv2.ExchangePeerResponse, error) {
	// Collect ExchangePeerCount metrics.
	metrics.ExchangePeerCount.Inc()
	resp, err := s.service.ExchangePeer(ctx, req)
	if err != nil {
		// Collect ExchangePeerFailureCount metrics.
		metrics.ExchangePeerFailureCount.Inc()
		return nil, err
	}

	return resp, nil
}

// StatTask checks information of task.
func (s *schedulerServerV2) StatTask(ctx context.Context, req *schedulerv2.StatTaskRequest) (*commonv2.Task, error) {
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

// AnnounceHost announces host to scheduler.
func (s *schedulerServerV2) AnnounceHost(ctx context.Context, req *schedulerv2.AnnounceHostRequest) (*emptypb.Empty, error) {
	// Collect AnnounceHostCount metrics.
	metrics.AnnounceHostCount.WithLabelValues(req.Host.GetOs(), req.Host.GetPlatform(), req.Host.GetPlatformFamily(), req.Host.GetPlatformFamily(),
		req.Host.GetKernelVersion(), req.Host.Build.GetGitVersion(), req.Host.Build.GetGitCommit(), req.Host.Build.GetGoVersion(), req.Host.Build.GetPlatform()).Inc()
	if err := s.service.AnnounceHost(ctx, req); err != nil {
		// Collect AnnounceHostFailureCount metrics.
		metrics.AnnounceHostFailureCount.WithLabelValues(req.Host.GetOs(), req.Host.GetPlatform(), req.Host.GetPlatformFamily(), req.Host.GetPlatformFamily(),
			req.Host.GetKernelVersion(), req.Host.Build.GetGitVersion(), req.Host.Build.GetGitCommit(), req.Host.Build.GetGoVersion(), req.Host.Build.GetPlatform()).Inc()
		return nil, err
	}

	return new(emptypb.Empty), nil
}

// LeaveHost releases host in scheduler.
func (s *schedulerServerV2) LeaveHost(ctx context.Context, req *schedulerv2.LeaveHostRequest) (*emptypb.Empty, error) {
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
func (s *schedulerServerV2) SyncProbes(stream schedulerv2.Scheduler_SyncProbesServer) error {
	// Collect SyncProbesCount metrics.
	metrics.SyncProbesCount.Inc()
	if err := s.service.SyncProbes(stream); err != nil {
		// Collect SyncProbesFailureCount metrics.
		metrics.SyncProbesFailureCount.Inc()
		return err
	}

	return nil
}
